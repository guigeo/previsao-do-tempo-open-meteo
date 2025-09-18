import os
import pandas as pd
from datetime import datetime

def csvs_para_bronze(raw_dir="data/raw", caminho_bronze="data/bronze/clima"):
    """
    Varre todos os CSVs em raw_dir e grava em parquet particionado no caminho_bronze.
    Cada CSV vira um parquet dentro do particionamento ano/mes/dia.
    """
    if not os.path.exists(raw_dir):
        print(f"⚠️ Diretório não encontrado: {raw_dir}")
        return

    arquivos = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]

    if not arquivos:
        print("⚠️ Nenhum CSV encontrado no diretório raw.")
        return

    for file in arquivos:
        caminho_csv = os.path.join(raw_dir, file)
        print(f"📥 Processando {caminho_csv}")

        df = pd.read_csv(caminho_csv, sep=";")

        # Se não tiver coluna de data, usa a data de hoje
        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"])
        else:
            df["data"] = datetime.today()

        # Extrai partições
        ano = df["data"].dt.year.iloc[0]
        mes = df["data"].dt.month.iloc[0]
        dia = df["data"].dt.day.iloc[0]

        # Define pasta particionada
        pasta_particao = os.path.join(caminho_bronze, f"ano={ano}", f"mes={mes:02d}", f"dia={dia:02d}")
        os.makedirs(pasta_particao, exist_ok=True)

        # Nome do arquivo parquet (mesmo nome do CSV)
        nome_parquet = file.replace(".csv", ".parquet")
        caminho_parquet = os.path.join(pasta_particao, nome_parquet)

        # Salva parquet
        df.to_parquet(
            caminho_parquet,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        print(f"✅ {caminho_parquet} salvo")
