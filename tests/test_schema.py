"""Testes dos builders de schema (diário/horário)."""

from __future__ import annotations

import pandas as pd
import pytest

from src.openmeteo.errors import PayloadError
from src.openmeteo.schema import ORDEM_HORARIO, build_daily_df, build_hourly_df


def test_build_daily_df_renomeia_e_adiciona_municipio(daily_payload, row):
    df = build_daily_df(daily_payload, row)
    assert df.loc[0, "data"] == "2025-11-20"
    assert df.loc[0, "temp_max_c"] == 28.4
    assert df.loc[0, "codigo_tempo_wmo"] == 61
    for col in ["codigo_ibge", "nome", "nome_uf", "latitude", "longitude"]:
        assert col in df.columns
    assert df.loc[0, "codigo_ibge"] == 3550308


def test_build_daily_df_payload_vazio_levanta(row):
    with pytest.raises(PayloadError):
        build_daily_df({"daily": {}}, row)


def test_build_hourly_df_adiciona_codigo_ibge_e_fonte(hourly_payload, row):
    raw = pd.DataFrame(hourly_payload["hourly"])
    df = build_hourly_df(raw, row, fonte="archive")
    assert list(df.columns) == ORDEM_HORARIO
    assert df.loc[0, "data_hora"] == "2025-11-20T00:00"
    assert df.loc[0, "temperatura_c"] == 20.1
    assert (df["codigo_ibge"] == 3550308).all()
    assert (df["fonte"] == "archive").all()
    assert df.loc[0, "municipio"] == "São Paulo"


def test_build_hourly_df_marca_forecast(hourly_payload, row):
    raw = pd.DataFrame(hourly_payload["hourly"])
    df = build_hourly_df(raw, row, fonte="forecast")
    assert (df["fonte"] == "forecast").all()
