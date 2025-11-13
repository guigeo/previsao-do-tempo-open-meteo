# src/recupera_dados_api_hora.py
import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.tz import gettz

def get_clima_horario(lat: float, lon: float, tz_name: str = "America/Sao_Paulo") -> pd.DataFrame:
    """
    Retorna dados horários do D-1 via Open-Meteo (archive) com fallback para forecast (past_days=1)
    """
    tz = gettz(tz_name)
    d1 = (datetime.now(tz) - timedelta(days=1)).date().strftime("%Y-%m-%d")

    params_archive = {
        "latitude": lat,
        "longitude": lon,
        "start_date": d1,
        "end_date": d1,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": tz_name,
    }

    url_archive = "https://archive-api.open-meteo.com/v1/archive"
    r = requests.get(url_archive, params=params_archive, timeout=30)
    if r.status_code == 200 and r.json().get("hourly"):
        return pd.DataFrame(r.json()["hourly"])

    # Fallback: forecast (caso o archive ainda não tenha o D-1 indexado)
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
    js = r2.json()
    df = pd.DataFrame(js.get("hourly", {}))
    if "time" in df.columns:
        df = df[df["time"].str.startswith(d1)]
    return df
