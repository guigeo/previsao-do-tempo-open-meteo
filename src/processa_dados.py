import pandas as pd

# --- Mapeamento para renomear colunas ---
COLUNAS_TRADUZIDAS = {
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


def processar_clima(clima_json, row):
    """
    Converte JSON da API em DataFrame e adiciona colunas do município.
    """
    df_clima = pd.DataFrame(clima_json["daily"])
    df_clima.rename(columns=COLUNAS_TRADUZIDAS, inplace=True)

    # Adiciona colunas do CSV original
    for col in ["codigo_ibge", "nome", "nome_uf", "latitude", "longitude"]:
        df_clima[col] = row[col]

    return df_clima
