"""Gravação atômica de Parquet, skip idempotente e upload para S3."""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

import boto3
import pandas as pd

from .config import Settings

log = logging.getLogger(__name__)


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


def upload_to_s3(settings: Settings, caminho_local: Path, tipo: str, data_referencia: str) -> str:
    """Envia o Parquet para s3://{bucket}/raw/clima/{tipo}/date=YYYY-MM-DD/{arquivo}."""
    if not settings.s3_bucket:
        raise ValueError("S3_BUCKET não configurado no ambiente")
    caminho_local = Path(caminho_local)
    if not caminho_local.exists():
        raise FileNotFoundError(f"arquivo local não encontrado: {caminho_local}")

    prefix = f"raw/clima/{tipo}/date={data_referencia}/{caminho_local.name}"
    log.info("upload S3: s3://%s/%s", settings.s3_bucket, prefix)
    boto3.client("s3").upload_file(str(caminho_local), settings.s3_bucket, prefix)
    return f"s3://{settings.s3_bucket}/{prefix}"
