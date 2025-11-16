# src/recupera_dados_api_dia.py

import requests
import time
from datetime import datetime
from dateutil.tz import gettz


def get_clima_diario_por_data(lat, lon, dia_str, tentativas=5, espera_inicial=5):
    """
    Coleta dados DIÁRIOS para uma data específica (YYYY-MM-DD).
    Mantém retries com backoff exponencial.
    """

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": (
            "temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_max,apparent_temperature_min,"
            "precipitation_sum,rain_sum,snowfall_sum,"
            "windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,"
            "shortwave_radiation_sum,weathercode"
        ),
        "timezone": "America/Sao_Paulo",
        "start_date": dia_str,
        "end_date": dia_str,
    }

    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            print(
                f"Erro na tentativa {tentativa}/{tentativas} "
                f"para {dia_str} ({lat}, {lon}): {e}"
            )

            if tentativa < tentativas:
                espera = espera_inicial * tentativa
                print(f"Aguardando {espera}s antes da nova tentativa...")
                time.sleep(espera)
            else:
                raise
