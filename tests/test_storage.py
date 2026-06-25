"""Testes de gravação atômica, skip idempotente e upload S3 (retry/reuso de client)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pandas as pd
import pytest
from botocore.exceptions import EndpointConnectionError

from src.openmeteo import storage
from src.openmeteo.errors import StorageError
from src.openmeteo.storage import (
    already_materialized,
    output_path,
    upload_to_s3,
    write_parquet_atomic,
)


def test_output_path_nomes(settings):
    d = date(2025, 11, 20)
    assert output_path(settings, "diario", d).name == "dados_climaticos_diarios_20251120.parquet"
    assert output_path(settings, "horario", d).name == "dados_climaticos_horarios_20251120.parquet"


def test_write_parquet_atomic_grava_e_le(settings, tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    destino = tmp_path / "raw" / "diario" / "f.parquet"
    write_parquet_atomic(df, destino)
    assert destino.exists()
    assert not list(destino.parent.glob(".*.tmp"))  # sem resíduo
    pd.testing.assert_frame_equal(pd.read_parquet(destino), df)


def test_write_parquet_atomic_nao_deixa_parcial_em_falha(settings, tmp_path, monkeypatch):
    df = pd.DataFrame({"a": [1]})
    destino = tmp_path / "raw" / "diario" / "f.parquet"

    def boom(*_a, **_k):
        raise RuntimeError("falha simulada na escrita")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
    with pytest.raises(RuntimeError):
        write_parquet_atomic(df, destino)
    assert not destino.exists()
    assert not list(destino.parent.glob(".*.tmp"))


def test_already_materialized(tmp_path):
    p = tmp_path / "x.parquet"
    assert not already_materialized(p)
    p.write_bytes(b"conteudo")
    assert already_materialized(p)


def test_upload_to_s3_sem_bucket_levanta(settings, tmp_path):
    p = tmp_path / "f.parquet"
    p.write_bytes(b"x")
    with pytest.raises(ValueError):
        storage.upload_to_s3(settings, p, "diario", "2025-11-20")


class _FakeS3Client:
    """Stub de client boto3 para testar retry/reuso sem rede."""

    def __init__(self, falhas_antes_sucesso: int = 0, sempre_falha: bool = False):
        self.falhas_antes_sucesso = falhas_antes_sucesso
        self.sempre_falha = sempre_falha
        self.chamadas: list[tuple[str, str, str]] = []

    def upload_file(self, filename, bucket, key):
        self.chamadas.append((filename, bucket, key))
        if self.sempre_falha or len(self.chamadas) <= self.falhas_antes_sucesso:
            raise EndpointConnectionError(endpoint_url="https://s3.amazonaws.com")


def test_upload_to_s3_reusa_client_passado(settings, tmp_path):
    s = replace(settings, s3_bucket="meu-bucket")
    p = tmp_path / "f.parquet"
    p.write_bytes(b"x")
    client = _FakeS3Client()

    url = upload_to_s3(s, p, "diario", "2026-06-01", client=client)

    assert url == "s3://meu-bucket/raw/clima/diario/date=2026-06-01/f.parquet"
    assert len(client.chamadas) == 1


def test_upload_to_s3_retry_apos_falha_transiente(settings, tmp_path, monkeypatch):
    monkeypatch.setattr(storage.time, "sleep", lambda *_: None)
    s = replace(settings, s3_bucket="meu-bucket", max_retries=3, backoff_base=0.0, backoff_max=0.0)
    p = tmp_path / "f.parquet"
    p.write_bytes(b"x")
    client = _FakeS3Client(falhas_antes_sucesso=1)

    url = upload_to_s3(s, p, "diario", "2026-06-01", client=client)

    assert url.endswith("f.parquet")
    assert len(client.chamadas) == 2


def test_upload_to_s3_falha_persistente_levanta_storage_error(settings, tmp_path, monkeypatch):
    monkeypatch.setattr(storage.time, "sleep", lambda *_: None)
    s = replace(settings, s3_bucket="meu-bucket", max_retries=2, backoff_base=0.0, backoff_max=0.0)
    p = tmp_path / "f.parquet"
    p.write_bytes(b"x")
    client = _FakeS3Client(sempre_falha=True)

    with pytest.raises(StorageError):
        upload_to_s3(s, p, "diario", "2026-06-01", client=client)
    assert len(client.chamadas) == 2
