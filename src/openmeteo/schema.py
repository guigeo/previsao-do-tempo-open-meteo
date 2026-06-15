"""Mapas de rename, ordem de colunas e builders dos DataFrames diário/horário."""

from __future__ import annotations

import pandas as pd

from .errors import PayloadError

# --- DIÁRIO ---
COLUNAS_DIARIO = {
    "time": "data",
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "apparent_temperature_max": "sensacao_termica_max_c",
    "apparent_temperature_min": "sensacao_termica_min_c",
    "precipitation_sum": "precipitacao_total_mm",
    "rain_sum": "chuva_mm",
    "snowfall_sum": "neve_mm",
    "windspeed_10m_max": "vento_velocidade_max_kmh",
    "windgusts_10m_max": "rajadas_vento_max_kmh",
    "winddirection_10m_dominant": "vento_direcao_dominante_graus",
    "shortwave_radiation_sum": "radiacao_solar_mj_m2",
    "weathercode": "codigo_tempo_wmo",
}

COLUNAS_MUNICIPIO_DIARIO = ["codigo_ibge", "nome", "nome_uf", "latitude", "longitude"]

# --- HORÁRIO ---
COLUNAS_HORARIO = {
    "time": "data_hora",
    "temperature_2m": "temperatura_c",
    "relative_humidity_2m": "umidade_relativa",
    "precipitation": "precipitacao_mm",
    "wind_speed_10m": "velocidade_vento_ms",
}

ORDEM_HORARIO = [
    "data_hora",
    "codigo_ibge",
    "municipio",
    "uf",
    "latitude",
    "longitude",
    "temperatura_c",
    "umidade_relativa",
    "precipitacao_mm",
    "velocidade_vento_ms",
    "fonte",
]


def build_daily_df(payload: dict, row) -> pd.DataFrame:
    """Converte o JSON diário da API em DataFrame com colunas do município."""
    daily = payload.get("daily")
    if not daily:
        raise PayloadError("payload diário sem chave 'daily'")
    df = pd.DataFrame(daily)
    df = df.rename(columns=COLUNAS_DIARIO)
    for col in COLUNAS_MUNICIPIO_DIARIO:
        df[col] = row[col]
    return df


def build_hourly_df(raw_df: pd.DataFrame, row, fonte: str) -> pd.DataFrame:
    """Enriquece o DataFrame horário com município, codigo_ibge, fonte e ordena colunas."""
    df = raw_df.copy()
    df["codigo_ibge"] = row["codigo_ibge"]
    df["municipio"] = row["nome"]
    df["uf"] = row["nome_uf"]
    df["latitude"] = row["latitude"]
    df["longitude"] = row["longitude"]
    df["fonte"] = fonte
    df = df.rename(columns=COLUNAS_HORARIO)
    return df[[c for c in ORDEM_HORARIO if c in df.columns]]
