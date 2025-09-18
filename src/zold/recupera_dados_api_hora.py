import requests
import pandas as pd
from tqdm import tqdm
from datetime import date
import time
import os

# --- Função com retry e backoff ---
def get_clima(lat, lon, tentativas=5):
    url = "https://api.open-meteo.com/v1/forecast"
    hoje = date.today().strftime("%Y-%m-%d")
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": (
            "temperature_2m,apparent_temperature,relative_humidity_2m,"
            "dew_point_2m,precipitation,rain,showers,snowfall,snow_depth,"
            "windspeed_10m,winddirection_10m,windgusts_10m,weathercode,"
            "visibility,is_day,soil_temperature_0cm,soil_moisture_0_1cm"
        ),
        "timezone": "America/Sao_Paulo",
        "start_date": hoje,
        "end_date": hoje
    }
    for i in range(tentativas):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Tentativa {i+1} falhou ({lat},{lon}): {e}")
            time.sleep(5 * (i + 1))  # espera progressiva
    return None

# --- Mapeamento para renomear colunas ---
colunas_traduzidas = {
    "time": "data_hora",
    "temperature_2m": "temperatura_2m_c",
    "apparent_temperature": "sensacao_termica_c",
    "relative_humidity_2m": "umidade_relativa_percent",
    "dew_point_2m": "ponto_orvalho_c",
    "precipitation": "precipitacao_mm",
    "rain": "chuva_mm",
    "showers": "pancadas_chuva_mm",
    "snowfall": "neve_mm",
    "snow_depth": "profundidade_neve_cm",
    "windspeed_10m": "vento_velocidade_10m_kmh",
    "winddirection_10m": "vento_direcao_10m_graus",
    "windgusts_10m": "rajadas_vento_10m_kmh",
    "weathercode": "codigo_tempo_wmo",
    "visibility": "visibilidade_metros",
    "is_day": "indicador_dia",
    "soil_temperature_0cm": "temperatura_solo_0cm_c",
    "soil_moisture_0_1cm": "umidade_solo_0_1cm_m3m3"
}

# --- Caminhos ---
df_cidades = pd.read_csv(
    "/home/gui_geo/projetos/previsao_tempo_open_meteo/data/lista_municipios/lista_mun.csv",
    sep=";"
)
path_ext_raw = "/home/gui_geo/projetos/previsao_tempo_open_meteo/data/extracao_raw/"
dt_str = date.today().strftime("%Y%m%d")
arquivo_saida = os.path.join(path_ext_raw, f"dados_climaticos_{dt_str}.csv")

# --- Retomada: verifica se já existe saída parcial ---
if os.path.exists(arquivo_saida):
    df_final = pd.read_csv(arquivo_saida, sep=";")
    processados = set(df_final["codigo_ibge"].unique())
    print(f"Retomando... {len(processados)} municípios já processados.")
else:
    df_final = pd.DataFrame()
    processados = set()

# --- Loop em lotes ---
batch_size = 50
dados = []

for idx, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0]):
    if row["codigo_ibge"] in processados:
        continue  # já foi feito, pula

    clima = get_clima(row["latitude"], row["longitude"])
    if clima:
        df_clima = pd.DataFrame(clima["hourly"])
        df_clima.rename(columns=colunas_traduzidas, inplace=True)

        # Adiciona colunas do CSV original
        for col in df_cidades.columns:
            df_clima[col] = row[col]

        dados.append(df_clima)

    # --- Salva a cada batch ---
    if (idx + 1) % batch_size == 0:
        if dados:
            df_parcial = pd.concat([df_final] + dados, ignore_index=True)
            df_parcial.to_csv(arquivo_saida, sep=";", index=False, encoding="utf-8")
            print(f"Salvo parcial até {idx+1} municípios.")
            df_final = df_parcial
            dados = []
        print("Pausa de 60 segundos...")
        time.sleep(60)

# --- Salva final ---
if dados:  # salva últimos registros
    df_final = pd.concat([df_final] + dados, ignore_index=True)
    df_final.to_csv(arquivo_saida, sep=";", index=False, encoding="utf-8")

print(f"Processo finalizado! Arquivo salvo em {arquivo_saida}")
