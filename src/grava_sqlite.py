import pandas as pd
import sqlite3
import os

#caminho_csv = '/home/gui_geo/projetos/previsao_tempo_open_meteo/data/extracao_raw/dados_climaticos_diarios_20250918.csv'
def grava_csv_sqlite(caminho_csv, caminho_banco="data/clima.db"):
    """Lê um CSV e grava no SQLite (append)."""
    if not caminho_csv or not os.path.exists(caminho_csv):
        print(f"Arquivo CSV não encontrado: {caminho_csv}")
        return

    # Lê CSV
    df = pd.read_csv(caminho_csv, sep=";")

    # Conexão SQLite
    os.makedirs(os.path.dirname(caminho_banco), exist_ok=True)
    conn = sqlite3.connect(caminho_banco)
    df.to_sql("clima_raw", conn, if_exists="append", index=False)
    conn.close()

    print(f"Dados do CSV {caminho_csv} gravados em {caminho_banco} (tabela clima_raw)")
#grava_csv_sqlite(caminho_csv, caminho_banco="data/clima.db")
