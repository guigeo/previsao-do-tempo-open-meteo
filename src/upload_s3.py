import boto3
from pathlib import Path
import os

BUCKET = os.getenv("S3_BUCKET")

def upload_para_s3(
    caminho_local: str | Path,
    tipo: str,                # "diarios" ou "horarios"
    data_referencia: str,     # "YYYY-MM-DD"
    bucket: str = BUCKET
):
    """
    Envia um arquivo Parquet para o S3 no prefixo:
      raw/clima/{tipo}/date=YYYY-MM-DD/{nome_arquivo}

    tipo: "diarios" ou "horarios"
    data_referencia: string YYYY-MM-DD
    """

    caminho_local = Path(caminho_local)
    if not caminho_local.exists():
        raise FileNotFoundError(f"Arquivo local não encontrado: {caminho_local}")

    # Nome do arquivo
    nome_arquivo = caminho_local.name

    # Prefixo S3 no padrão Hive style
    prefix = f"raw/clima/{tipo}/date={data_referencia}/{nome_arquivo}"

    print(f".  Enviando para S3:")
    print(f"   Bucket: {bucket}")
    print(f"   Prefixo: {prefix}")
    print(f"   Arquivo: {caminho_local}")

    # Sessão boto3 usando o profile configurado 
    #session = boto3.Session(profile_name=profile)
    s3 = boto3.client("s3")

    # Upload (sobrescreve automaticamente se já existir)
    s3.upload_file(str(caminho_local), bucket, prefix)

    print("Upload concluído com sucesso!\n")

    return f"s3://{bucket}/{prefix}"
