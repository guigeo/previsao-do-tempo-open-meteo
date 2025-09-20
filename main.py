import pandas as pd
import os
from datetime import date
from tqdm import tqdm

from src.recupera_dados_api_dia import get_clima_diario
from src.processa_dados import processar_clima
from src.grava_sqlite import grava_csv_sqlite
from src.grava_bronze import grava_csv_bronze


def main():
    # --- Carrega lista de municípios ---
    df_cidades = pd.read_csv(
        "/home/gui_geo/projetos/previsao_tempo_open_meteo/data/lista_municipios/lista_mun.csv",
        sep=";"
    )

    path_ext_raw = "/home/gui_geo/projetos/previsao_tempo_open_meteo/data/raw/"
    dt_str = date.today().strftime("%Y%m%d")

    print(f"Buscando dados de HOJE ({dt_str}) para {len(df_cidades)} municípios")

    # --- Loop nos municípios ---
    dados = []
    for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0]):
        try:
            clima = get_clima_diario(row["latitude"], row["longitude"])
            df_clima = processar_clima(clima, row)
            dados.append(df_clima)
        except Exception as e:
            print(f"Falha definitiva em {row['nome']} ({row['nome_uf']}): {e}")

    # --- Consolida ---
    if dados:
        df_final = pd.concat(dados, ignore_index=True)
        os.makedirs(path_ext_raw, exist_ok=True)

        saida = os.path.join(path_ext_raw, f"dados_climaticos_diarios_{dt_str}.csv")
        df_final.to_csv(saida, sep=";", index=False, encoding="utf-8")

        print(f"Processo finalizado! Dados salvos em {saida}")

    else:
        print("Nenhum dado coletado.")
    
    # --- Grava também no banco SQLite ---
    arquivo_csv = (path_ext_raw + f"dados_climaticos_diarios_{dt_str}.csv")
    caminho_banco = "data/clima.db"
    
    BASE_DIR = "/home/gui_geo/projetos/previsao_tempo_open_meteo"

    caminho_banco = os.path.join(BASE_DIR, "data/clima.db")
    caminho_bronze = os.path.join(BASE_DIR, "data/bronze/clima")

    grava_csv_sqlite(arquivo_csv, caminho_banco)
    grava_csv_bronze(arquivo_csv, caminho_bronze)
    
if __name__ == "__main__":
    main()
