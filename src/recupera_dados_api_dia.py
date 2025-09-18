import requests
import time
from datetime import date


def get_clima_diario(lat, lon, tentativas=5, espera_inicial=5):
    """
    Faz requisição à API do Open-Meteo com retries e backoff exponencial.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    hoje = date.today().strftime("%Y-%m-%d")
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
        "start_date": hoje,
        "end_date": hoje
    }

    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"⚠️ Erro na tentativa {tentativa}/{tentativas} "
                  f"para coordenadas ({lat}, {lon}): {e}")
            if tentativa < tentativas:
                espera = espera_inicial * tentativa
                print(f"⏳ Aguardando {espera}s antes da nova tentativa...")
                time.sleep(espera)
            else:
                raise
