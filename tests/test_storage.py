"""Testes de gravação atômica e skip idempotente."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.openmeteo import storage
from src.openmeteo.storage import (
    already_materialized,
    output_path,
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
