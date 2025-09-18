import requests
import pandas as pd
from datetime import date
import os
from tqdm import tqdm

# --- Função para buscar o dia de hoje ---
def get_clima_diario(lat, lon):
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
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()

# --- Mapeamento para renomear colunas ---
colunas_traduzidas = {
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
    "weathercode": "codigo_tempo_wmo"
}

# --- Caminhos dentro do container ---
BASE_PATH = "/app/data"
csv_path = os.path.join(BASE_PATH, "lista_municipios", "lista_mun.csv")
path_ext_raw = os.path.join(BASE_PATH, "extracao_raw")
dt_str = date.today().strftime("%Y%m%d")

# --- Carregar municípios ---
df_cidades = pd.read_csv(csv_path, sep=";")

# 🔎 Filtro: apenas SP
df_cidades = df_cidades[df_cidades["nome_uf"] == "São Paulo"].reset_index(drop=True)

print(f"📌 Buscando dados de HOJE ({dt_str}) para municípios de SP ({len(df_cidades)} cidades)")

# --- Loop nos municípios ---
dados = []
for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0]):
    try:
        clima = get_clima_diario(row["latitude"], row["longitude"])
        df_clima = pd.DataFrame(clima["daily"])
        df_clima.rename(columns=colunas_traduzidas, inplace=True)

        # Adiciona colunas do CSV original
        for col in ["codigo_ibge", "nome", "nome_uf", "latitude", "longitude"]:
            df_clima[col] = row[col]

        dados.append(df_clima)
    except Exception as e:
        print(f"⚠️ Erro em {row['nome']}: {e}")

# --- Consolida ---
if dados:
    df_final = pd.concat(dados, ignore_index=True)
    os.makedirs(path_ext_raw, exist_ok=True)

    saida = os.path.join(path_ext_raw, f"dados_climaticos_diarios_SP_{dt_str}.csv")
    df_final.to_csv(saida, sep=";", index=False, encoding="utf-8")

    print(f"✅ Processo finalizado! Dados salvos em {saida}")
else:
    print("Nenhum dado coletado.")
