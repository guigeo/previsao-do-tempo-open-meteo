"""Fixtures compartilhadas dos testes da camada de extração."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.openmeteo.config import Settings


@pytest.fixture
def row():
    """Linha de município no formato do lista_mun.csv."""
    return pd.Series({
        "codigo_ibge": 3550308,
        "nome": "São Paulo",
        "nome_uf": "São Paulo",
        "latitude": -23.5505,
        "longitude": -46.6333,
    })


@pytest.fixture
def daily_payload():
    """Payload diário típico (archive) com 1 dia."""
    return {
        "daily": {
            "time": ["2025-11-20"],
            "temperature_2m_max": [28.4],
            "temperature_2m_min": [18.1],
            "apparent_temperature_max": [30.0],
            "apparent_temperature_min": [17.5],
            "precipitation_sum": [2.3],
            "rain_sum": [2.3],
            "snowfall_sum": [0.0],
            "windspeed_10m_max": [15.2],
            "windgusts_10m_max": [30.1],
            "winddirection_10m_dominant": [120],
            "shortwave_radiation_sum": [21.5],
            "weathercode": [61],
        }
    }


@pytest.fixture
def hourly_payload():
    """Payload horário típico (archive) com 2 horas."""
    return {
        "hourly": {
            "time": ["2025-11-20T00:00", "2025-11-20T01:00"],
            "temperature_2m": [20.1, 19.8],
            "relative_humidity_2m": [80, 82],
            "precipitation": [0.0, 0.1],
            "wind_speed_10m": [5.4, 4.9],
        }
    }


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings apontando para um base_dir temporário com a estrutura mínima."""
    (tmp_path / "data" / "lista_municipios").mkdir(parents=True)
    (tmp_path / "state").mkdir(parents=True)
    csv = tmp_path / "data" / "lista_municipios" / "lista_mun.csv"
    csv.write_text(
        "codigo_ibge;nome;nome_uf;latitude;longitude\n"
        "3550308;São Paulo;São Paulo;-23.5505;-46.6333\n"
    )
    return Settings.load(base_dir=tmp_path)
