"""Extração diária/horária da Open-Meteo: monta params, valida payload e marca proveniência."""

from __future__ import annotations

import pandas as pd

from .client import OpenMeteoClient
from .config import Settings
from .errors import PayloadError

_DAILY_VARS = (
    "temperature_2m_max,temperature_2m_min,"
    "apparent_temperature_max,apparent_temperature_min,"
    "precipitation_sum,rain_sum,snowfall_sum,"
    "windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,"
    "shortwave_radiation_sum,weathercode"
)
_HOURLY_VARS = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"


def _raise_on_api_error(payload: dict) -> None:
    if isinstance(payload, dict) and payload.get("error"):
        raise PayloadError(payload.get("reason", "erro estruturado da API"))


def extract_daily(client: OpenMeteoClient, settings: Settings, lat, lon, dia_str: str) -> dict:
    """Retorna o payload diário (archive) validado para uma data."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": _DAILY_VARS,
        "timezone": settings.timezone,
        "start_date": dia_str,
        "end_date": dia_str,
    }
    payload = client.get_json(settings.archive_url, params)
    _raise_on_api_error(payload)
    if not payload.get("daily"):
        raise PayloadError(f"sem dados diários para {dia_str} ({lat},{lon})")
    return payload


def extract_hourly(client: OpenMeteoClient, settings: Settings, lat, lon, dia_str: str) -> tuple[pd.DataFrame, str]:
    """Retorna (DataFrame horário, fonte) usando archive; cai em forecast como fallback."""
    archive_params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": dia_str,
        "end_date": dia_str,
        "hourly": _HOURLY_VARS,
        "timezone": settings.timezone,
    }
    payload = client.get_json(settings.archive_url, archive_params)
    _raise_on_api_error(payload)
    if payload.get("hourly"):
        df = pd.DataFrame(payload["hourly"])
        if not df.empty:
            return df, "archive"

    forecast_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": _HOURLY_VARS,
        "past_days": 1,
        "timezone": settings.timezone,
    }
    fallback = client.get_json(settings.forecast_url, forecast_params)
    _raise_on_api_error(fallback)
    df = pd.DataFrame(fallback.get("hourly", {}))
    if "time" in df.columns:
        df = df[df["time"].str.startswith(dia_str)]
    if df.empty:
        raise PayloadError(f"sem dados horários para {dia_str} ({lat},{lon})")
    return df, "forecast"
