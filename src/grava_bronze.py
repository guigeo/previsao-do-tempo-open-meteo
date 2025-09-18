import os
import pandas as pd
from datetime import datetime

def grava_csv_bronze(caminho_csv, caminho_bronze="data/bronze/clima"):
    """
    Converte um único CSV para parquet particionado (ano/mes/dia).
    """
    if not os.path.exists(caminho_csv):
        print(f"Arquivo não encontrado: {caminho_csv}")
        return

    df = pd.read_csv(caminho_csv, sep=";")

    # Se não tiver coluna de data, usa data de hoje
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"])
    else:
        df["data"] = datetime.today()

    # Extrai partições
    ano = df["data"].dt.year.iloc[0]
    mes = df["data"].dt.month.iloc[0]
    dia = df["data"].dt.day.iloc[0]

    # Define pasta de saída
    pasta_particao = os.path.join(caminho_bronze, f"ano={ano}", f"mes={mes:02d}", f"dia={dia:02d}")
    os.makedirs(pasta_particao, exist_ok=True)

    # Nome do parquet = mesmo nome do CSV
    nome_parquet = os.path.basename(caminho_csv).replace(".csv", ".parquet")
    caminho_parquet = os.path.join(pasta_particao, nome_parquet)

    # Salva parquet
    df.to_parquet(caminho_parquet, engine="pyarrow", compression="snappy", index=False)

    print(f"Bronze salvo em {caminho_parquet}")
