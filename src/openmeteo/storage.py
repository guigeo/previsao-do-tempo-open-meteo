"""Gravação atômica de Parquet, skip idempotente e upload para S3."""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import date
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

from .config import Settings
from .errors import StorageError

log = logging.getLogger(__name__)

_S3_RETRYABLE_EXC = (BotoCoreError, ClientError)


def output_path(settings: Settings, tipo: str, dia: date) -> Path:
    """Caminho local do parquet do dia (1 arquivo por dia/tipo)."""
    nome = {
        "diario": f"dados_climaticos_diarios_{dia:%Y%m%d}.parquet",
        "horario": f"dados_climaticos_horarios_{dia:%Y%m%d}.parquet",
    }[tipo]
    return settings.raw_dir(tipo) / nome


def already_materialized(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def write_parquet_atomic(df: pd.DataFrame, final_path: Path, *, compression: str = "snappy") -> Path:
    """Escreve em arquivo temporário no mesmo diretório e renomeia (atômico no mesmo volume)."""
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = final_path.with_name(f".{final_path.name}.tmp")
    if tmp.exists():
        tmp.unlink()
    try:
        df.to_parquet(tmp, index=False, engine="pyarrow", compression=compression)
        os.replace(tmp, final_path)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
    return final_path


def _backoff(settings: Settings, attempt: int) -> float:
    base = settings.backoff_base * (2 ** (attempt - 1))
    return min(base, settings.backoff_max) + random.uniform(0, 0.5)


def upload_to_s3(
    settings: Settings,
    caminho_local: Path,
    tipo: str,
    data_referencia: str,
    *,
    client=None,
) -> str:
    """Envia o Parquet para s3://{bucket}/raw/clima/{tipo}/date=YYYY-MM-DD/{arquivo}.

    Aceita um client boto3 opcional para reuso em uploads em lote (evita reabrir
    conexão/credenciais por arquivo); usa retry/backoff igual ao client HTTP da API.
    """
    if not settings.s3_bucket:
        raise ValueError("S3_BUCKET não configurado no ambiente")
    caminho_local = Path(caminho_local)
    if not caminho_local.exists():
        raise FileNotFoundError(f"arquivo local não encontrado: {caminho_local}")

    prefix = f"raw/clima/{tipo}/date={data_referencia}/{caminho_local.name}"
    s3 = client or boto3.client("s3")

    last_exc: Exception | None = None
    for attempt in range(1, settings.max_retries + 1):
        try:
            s3.upload_file(str(caminho_local), settings.s3_bucket, prefix)
            log.info("upload S3: s3://%s/%s", settings.s3_bucket, prefix)
            return f"s3://{settings.s3_bucket}/{prefix}"
        except _S3_RETRYABLE_EXC as exc:
            last_exc = exc
            if attempt >= settings.max_retries:
                break
            wait = _backoff(settings, attempt)
            log.warning("falha upload S3 (tentativa %d/%d): %s; aguardando %.1fs",
                        attempt, settings.max_retries, exc, wait)
            time.sleep(wait)

    raise StorageError(
        f"upload S3 falhou após {settings.max_retries} tentativas ({prefix}): {last_exc}"
    ) from last_exc
