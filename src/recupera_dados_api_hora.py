# src/recupera_dados_api_hora.py

import requests
import pandas as pd
from datetime import datetime
from dateutil.tz import gettz


def get_clima_horario_por_data(lat: float, lon: float, dia_str: str,
                                tz_name: str = "America/Sao_Paulo") -> pd.DataFrame:
    """
    Retorna dados horários para um dia específico (YYYY-MM-DD),
    usando archive; se archive ainda não tiver o dia, usa forecast (fallback).
    """

    # ---------- Tenta API Archive ----------
    params_archive = {
        "latitude": lat,
        "longitude": lon,
        "start_date": dia_str,
        "end_date": dia_str,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": tz_name,
    }

    url_archive = "https://archive-api.open-meteo.com/v1/archive"
    r = requests.get(url_archive, params=params_archive, timeout=30)

    if r.status_code == 200 and r.json().get("hourly"):
        return pd.DataFrame(r.json()["hourly"])

    # ---------- Fallback Forecast ----------
    params_forecast = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "past_days": 1,
        "timezone": tz_name,
    }

    url_forecast = "https://api.open-meteo.com/v1/forecast"
    r2 = requests.get(url_forecast, params=params_forecast, timeout=30)
    r2.raise_for_status()

    df = pd.DataFrame(r2.json().get("hourly", {}))
    if "time" in df.columns:
        df = df[df["time"].str.startswith(dia_str)]

    return df
