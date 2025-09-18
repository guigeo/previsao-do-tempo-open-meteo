import pandas as pd
import os
from datetime import date
from tqdm import tqdm

from src.recupera_dados_api_dia import get_clima_diario
from src.processa_dados import processar_clima
from src.grava_sqlite import grava_csv_sqlite


def main():
    # --- Carrega lista de municípios ---
    df_cidades = pd.read_csv(
        "/home/gui_geo/projetos/previsao_tempo_open_meteo/data/lista_municipios/lista_mun.csv",
        sep=";"
    )

    path_ext_raw = "/home/gui_geo/projetos/previsao_tempo_open_meteo/data/extracao_raw/"
    dt_str = date.today().strftime("%Y%m%d")

    print(f"📌 Buscando dados de HOJE ({dt_str}) para {len(df_cidades)} municípios")

    # --- Loop nos municípios ---
    dados = []
    for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0]):
        try:
            clima = get_clima_diario(row["latitude"], row["longitude"])
            df_clima = processar_clima(clima, row)
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
    
    # --- Grava também no banco SQLite ---

    grava_banco = (path_ext_raw + f"dados_climaticos_diarios_{dt_str}.csv")
    caminho_banco = "data/clima.db"
    grava_csv_sqlite(grava_banco, caminho_banco)

if __name__ == "__main__":
    main()
