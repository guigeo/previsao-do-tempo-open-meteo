import requests
import pandas as pd
import time


def geocode(place):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": place,
        "format": "json",
        "limit": 1
    }
    resp = requests.get(url, params=params, headers={"User-Agent": "geo-teste"})
    resp.raise_for_status()
    result = resp.json()
    if result:
        lat = float(result[0]["lat"])
        lon = float(result[0]["lon"])
        return lat, lon
    else:
        return None, None


# --- Exemplo de chamadas ---
cidades = [
"São Paulo, São Paulo, Brasil",
"Guarulhos, São Paulo, Brasil",
"Santo André, São Paulo, Brasil",
"São Bernardo do Campo, São Paulo, Brasil",
"São Caetano do Sul, São Paulo, Brasil",
"Diadema, São Paulo, Brasil",
"Mauá, São Paulo, Brasil",
"Salvador, Bahia, Brasil"]

for cidade in cidades:
    lat, lon = geocode(cidade)
    print(f"Cidade: {cidade}")
    print(f"Latitude: {lat}, Longitude: {lon}")
    print("-" * 40)