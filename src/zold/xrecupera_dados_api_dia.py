import requests
import pandas as pd
from datetime import date
import os
from tqdm import tqdm
import time

# --- Função para buscar o dia de hoje com retry ---
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

# --- Caminhos ---
df_cidades = pd.read_csv(
    "/home/gui_geo/projetos/previsao_tempo_open_meteo/data/lista_municipios/lista_mun copy.csv",
    sep=";"
)

# 🔎 Agora pega todas as cidades da lista (sem filtro de SP)
path_ext_raw = "/home/gui_geo/projetos/previsao_tempo_open_meteo/data/extracao_raw/"
dt_str = date.today().strftime("%Y%m%d")

print(f"📌 Buscando dados de HOJE ({dt_str}) para {len(df_cidades)} municípios")

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
        print(f"❌ Falha definitiva em {row['nome']} ({row['nome_uf']}): {e}")

# --- Consolida ---
if dados:
    df_final = pd.concat(dados, ignore_index=True)
    os.makedirs(path_ext_raw, exist_ok=True)

    saida = os.path.join(path_ext_raw, f"dados_climaticos_diarios_{dt_str}.csv")
    df_final.to_csv(saida, sep=";", index=False, encoding="utf-8")

    print(f"✅ Processo finalizado! Dados salvos em {saida}")
else:
    print("Nenhum dado coletado.")
