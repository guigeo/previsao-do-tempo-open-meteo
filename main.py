# main.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, date
from dateutil.tz import gettz
import argparse
import pandas as pd
from tqdm import tqdm
import time
import os

# --- Suas libs locais ---
from src.recupera_dados_api_dia import get_clima_diario_por_data
from src.recupera_dados_api_hora import get_clima_horario_por_data
from src.processa_dados import processar_clima
from src.upload_s3 import upload_para_s3


TIMEZONE = "America/Sao_Paulo"


# ============================================================
# HELPERS
# ============================================================
def _resolve_base_dir() -> Path:
    """Acha raiz do projeto."""
    here = Path(__file__).resolve().parent
    for base in [here, here.parent]:
        if (base / "data" / "lista_municipios").exists():
            return base
    return here


def _hoje():
    tz = gettz(TIMEZONE)
    return datetime.now(tz).date()


def _d1():
    return _hoje() - timedelta(days=1)


# ---------- STATE FILE ----------
def _state_file(base_dir: Path) -> Path:
    return base_dir / "state" / "last_run.txt"


def _carregar_last_run(base_dir: Path):
    sf = _state_file(base_dir)
    if not sf.exists():
        return None  # primeira execução
    try:
        return datetime.strptime(sf.read_text().strip(), "%Y-%m-%d").date()
    except:
        return None


def _salvar_last_run(base_dir: Path, d: date):
    sf = _state_file(base_dir)
    sf.parent.mkdir(parents=True, exist_ok=True)
    sf.write_text(d.strftime("%Y-%m-%d"))


def _datas_pendentes(base_dir: Path):
    last_run = _carregar_last_run(base_dir)
    d1 = _d1()

    # primeira execução → processa só D-1
    if last_run is None:
        return [d1]

    dias = []
    dia = last_run + timedelta(days=1)
    while dia <= d1:
        dias.append(dia)
        dia += timedelta(days=1)

    return dias


# ============================================================
# COLETA DIÁRIA
# ============================================================
def coleta_diaria(base_dir: Path, dia: date):
    dt_str = dia.strftime("%Y-%m-%d")
    path_lista = base_dir / "data" / "lista_municipios" / "lista_mun.csv"
    path_ext_raw_diario = base_dir / "data" / "raw" / "diario"

    df_cidades = pd.read_csv(path_lista, sep=";")
    print(f"📅 (DIÁRIO) Coletando {dt_str} para {len(df_cidades)} municípios")

    dados = []
    falhas = 0

    for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0]):
        try:
            clima = get_clima_diario_por_data(row["latitude"], row["longitude"], dt_str)
            df_clima = processar_clima(clima, row)
            dados.append(df_clima)
        except Exception as e:
            falhas += 1
            print(f"Falha em {row['nome']} ({row['nome_uf']}): {e}")

    if not dados:
        print("❌ Nenhum dado diário coletado.")
        return None

    df_final = pd.concat(dados, ignore_index=True)

    os.makedirs(path_ext_raw_diario, exist_ok=True)
    saida = path_ext_raw_diario / f"dados_climaticos_diarios_{dia.strftime('%Y%m%d')}.parquet"

    df_final.to_parquet(saida, index=False, engine="pyarrow", compression="snappy")

    print(f"✅ Diário salvo em: {saida}")
    if falhas:
        print(f"Atenção: {falhas} município(s) falharam (diário).")

    return saida


# ============================================================
# COLETA HORÁRIA
# ============================================================
def coleta_horaria(base_dir: Path, dia: date):
    dt_str = dia.strftime("%Y-%m-%d")
    dt_file = dia.strftime("%Y%m%d")

    path_lista = base_dir / "data" / "lista_municipios" / "lista_mun.csv"
    path_ext_raw_horario = base_dir / "data" / "raw" / "horario"

    df_cidades = pd.read_csv(path_lista, sep=";")
    print(f"⏱️ (HORÁRIO) Coletando {dt_str} para {len(df_cidades)} municípios")

    dados = []
    falhas = 0

    for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0]):
        try:
            df_hora = get_clima_horario_por_data(row["latitude"], row["longitude"], dt_str, TIMEZONE)
            df_hora["municipio"] = row["nome"]
            df_hora["uf"] = row["nome_uf"]
            df_hora["latitude"] = row["latitude"]
            df_hora["longitude"] = row["longitude"]
            dados.append(df_hora)
        except Exception as e:
            falhas += 1
            print(f"Falha em {row['nome']}: {e}")

    if not dados:
        print("❌ Nenhum dado horário coletado.")
        return None

    df_final = pd.concat(dados, ignore_index=True)

    # renomeia
    rename_cols = {
        "time": "data_hora",
        "temperature_2m": "temperatura_c",
        "relative_humidity_2m": "umidade_relativa",
        "precipitation": "precipitacao_mm",
        "wind_speed_10m": "velocidade_vento_ms",
    }
    df_final.rename(columns=rename_cols, inplace=True)

    colunas = [
        "data_hora", "municipio", "uf", "latitude", "longitude",
        "temperatura_c", "umidade_relativa", "precipitacao_mm",
        "velocidade_vento_ms"
    ]
    df_final = df_final[[c for c in colunas if c in df_final.columns]]

    os.makedirs(path_ext_raw_horario, exist_ok=True)
    saida = path_ext_raw_horario / f"dados_climaticos_horarios_{dt_file}.parquet"

    df_final.to_parquet(saida, index=False, engine="pyarrow", compression="snappy")

    print(f"✅ Horário salvo em: {saida}")
    if falhas:
        print(f"Atenção: {falhas} município(s) falharam (horário).")

    return saida


# ============================================================
# MAIN
# ============================================================
def parse_args():
    p = argparse.ArgumentParser(description="Coleta Open-Meteo – diário/horário/ambos (incremental)")
    p.add_argument("--modo", choices=["diario", "horario", "ambos"], default="ambos")
    return p.parse_args()


def main():
    args = parse_args()
    base_dir = _resolve_base_dir()

    print("📁 BASE_DIR:", base_dir)

    datas = _datas_pendentes(base_dir)

    if not datas:
        print("Nenhuma data pendente. Nada a fazer.")
        return

    print("📅 Datas a processar:", [str(d) for d in datas])

    for dia in datas:
        print(f"\n==============================")
        print(f"PROCESSANDO {dia}")
        print(f"==============================")

        # --- DIÁRIO ---
        if args.modo in ("diario", "ambos"):
            p1 = coleta_diaria(base_dir, dia)
            if p1:
                upload_para_s3(
                    caminho_local=p1,
                    tipo="diario",
                    data_referencia=dia.strftime("%Y-%m-%d")
                )

        # --- HORÁRIO ---
        if args.modo in ("horario", "ambos"):
            p2 = coleta_horaria(base_dir, dia)
            if p2:
                upload_para_s3(
                    caminho_local=p2,
                    tipo="horario",
                    data_referencia=dia.strftime("%Y-%m-%d")
                )

        # Atualiza state
        _salvar_last_run(base_dir, dia)
        print(f"📌 STATE atualizado para {dia}")


if __name__ == "__main__":
    main()
