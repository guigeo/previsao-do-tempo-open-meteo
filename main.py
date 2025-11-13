# main.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from dateutil.tz import gettz
import argparse
import pandas as pd
from tqdm import tqdm
import time
import os

# --- Suas libs locais ---
from src.recupera_dados_api_dia import get_clima_diario
from src.recupera_dados_api_hora import get_clima_horario
from src.processa_dados import processar_clima

# ============================================================
# CONFIG PADRÃO (pode mudar aqui)
# ============================================================
MODO_COLETA_DEFAULT = "ambos"  # "diario" | "horario" | "ambos"
TIMEZONE = "America/Sao_Paulo"

# ============================================================
# HELPERS
# ============================================================
def _resolve_base_dir() -> Path:
    """Resolve a raiz do projeto de forma robusta."""
    here = Path(__file__).resolve().parent
    candidates = [here, here.parent]
    for base in candidates:
        if (base / "data" / "lista_municipios").exists():
            return base
    return here

def _d1_date(tz_name: str = TIMEZONE):
    tz = gettz(tz_name)
    return (datetime.now(tz) - timedelta(days=1)).date()

def _load_cidades(path_lista: Path) -> pd.DataFrame:
    if not path_lista.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path_lista}")
    return pd.read_csv(path_lista, sep=";")

# ============================================================
# COLETA DIÁRIA
# ============================================================
def coleta_diaria(base_dir: Path, dt_str: str):
    path_lista = base_dir / "data" / "lista_municipios" / "lista_mun.csv"
    # salva em data/raw/diario
    path_ext_raw_diario = base_dir / "data" / "raw" / "diario"

    df_cidades = _load_cidades(path_lista)
    print(f"📅 Coletando dados DIÁRIOS ({dt_str}) para {len(df_cidades)} municípios")

    dados = []
    falhas = 0

    for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0]):
        tentativa, sucesso = 0, False
        while tentativa < 2 and not sucesso:
            try:
                clima = get_clima_diario(row["latitude"], row["longitude"])
                df_clima = processar_clima(clima, row)
                dados.append(df_clima)
                sucesso = True
            except Exception as e:
                tentativa += 1
                if tentativa >= 2:
                    falhas += 1
                    print(f"Falha definitiva em {row.get('nome')} ({row.get('nome_uf')}): {e}")
                else:
                    time.sleep(1)

    if not dados:
        print("❌ Nenhum dado diário coletado.")
        return None

    df_final = pd.concat(dados, ignore_index=True)
    os.makedirs(path_ext_raw_diario, exist_ok=True)
    saida = path_ext_raw_diario / f"dados_climaticos_diarios_{dt_str}.parquet"

    # 👉 salva em Parquet
    df_final.to_parquet(saida, index=False, engine="pyarrow", compression="snappy")

    print(f"✅ Dados diários salvos (Parquet) em: {saida}")
    if falhas:
        print(f"Atenção: {falhas} município(s) falharam (diário).")

    return saida

# ============================================================
# COLETA HORÁRIA
# ============================================================
def coleta_horaria(base_dir: Path, dt_str: str):
    path_lista = base_dir / "data" / "lista_municipios" / "lista_mun.csv"
    # salva em data/raw/horario
    path_ext_raw_horario = base_dir / "data" / "raw" / "horario"

    df_cidades = _load_cidades(path_lista)
    print(f"⏱️ Coletando dados HORÁRIOS ({dt_str}) para {len(df_cidades)} municípios")

    dados = []
    falhas = 0

    for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0]):
        tentativa, sucesso = 0, False
        while tentativa < 2 and not sucesso:
            try:
                df_hora = get_clima_horario(row["latitude"], row["longitude"], TIMEZONE)
                # Enriquecimento com metadados da cidade
                df_hora["nome"] = row["nome"]
                df_hora["uf"] = row["nome_uf"]
                df_hora["latitude"] = row["latitude"]
                df_hora["longitude"] = row["longitude"]
                dados.append(df_hora)
                sucesso = True
            except Exception as e:
                tentativa += 1
                if tentativa >= 2:
                    falhas += 1
                    print(f"Falha definitiva em {row.get('nome')} ({row.get('nome_uf')}): {e}")
                else:
                    time.sleep(1)

    if not dados:
        print("❌ Nenhum dado horário coletado.")
        return None

    df_final = pd.concat(dados, ignore_index=True)

    # Padroniza cabeçalho (PT-BR) e ordem
    rename_cols = {
        "time": "data_hora",
        "temperature_2m": "temperatura_c",
        "relative_humidity_2m": "umidade_relativa",
        "precipitation": "precipitacao_mm",
        "wind_speed_10m": "velocidade_vento_ms",
        "nome": "municipio",
        "uf": "uf",
        "latitude": "latitude",
        "longitude": "longitude",
    }
    df_final.rename(columns=rename_cols, inplace=True)
    colunas_ordenadas = [
        "data_hora", "municipio", "uf", "latitude", "longitude",
        "temperatura_c", "umidade_relativa", "precipitacao_mm", "velocidade_vento_ms",
    ]
    df_final = df_final[[c for c in colunas_ordenadas if c in df_final.columns]]

    os.makedirs(path_ext_raw_horario, exist_ok=True)
    saida = path_ext_raw_horario / f"dados_climaticos_horarios_{dt_str}.parquet"

    # 👉 salva em Parquet
    df_final.to_parquet(saida, index=False, engine="pyarrow", compression="snappy")

    print(f"✅ Dados horários salvos (Parquet) em: {saida}")
    if falhas:
        print(f"Atenção: {falhas} município(s) falharam (horário).")

    return saida


# ============================================================
# MAIN
# ============================================================
def parse_args():
    p = argparse.ArgumentParser(description="Coleta Open-Meteo (D-1) – diário/horário/ambos")
    p.add_argument("--modo", choices=["diario", "horario", "ambos"], default=MODO_COLETA_DEFAULT,
                   help="Modo de coleta (padrão: ambos)")
    return p.parse_args()

def main():
    args = parse_args()
    base_dir = _resolve_base_dir()
    dt_str = _d1_date(TIMEZONE).strftime("%Y%m%d")

    print("📁 BASE_DIR:", base_dir)
    print(f"🕓 Coletando D-1 ({dt_str}) | modo={args.modo}")

    paths = []

    if args.modo in ("diario", "ambos"):
        p1 = coleta_diaria(base_dir, dt_str)
        if p1: paths.append(p1)

    if args.modo in ("horario", "ambos"):
        p2 = coleta_horaria(base_dir, dt_str)
        if p2: paths.append(p2)

    if paths:
        print("\nArquivos gerados:")
        for p in paths:
            print(" •", p)
    else:
        print("\nNenhum arquivo gerado.")

if __name__ == "__main__":
    main()
