# scripts/backfill_once.py
from __future__ import annotations

# --- garantir que 'src' seja importável sem mexer no main.py ---
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]  # raiz do projeto
sys.path.append(str(ROOT))

import time
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from dateutil.tz import gettz
from tqdm import tqdm

# usa sua padronização diária existente
from src.processa_dados import processar_clima

# ======== CONFIG ONE-OFF ========
DATA_INI = date(2025, 11, 1)
DATA_FIM = date(2025, 11, 10)
TIMEZONE = "America/Sao_Paulo"
HTTP_TIMEOUT = 30
RETRIES = 2
SLEEP_BETWEEN_CALLS = 0.02  # folga leve entre chamadas
PARQUET_COMPRESSION = "snappy"
PARQUET_ENGINE = "pyarrow"
# ================================

def base_dir() -> Path:
    """Raiz do repo (onde existe pasta data/lista_municipios)."""
    here = ROOT
    if not (here / "data" / "lista_municipios").exists():
        here = Path(__file__).resolve().parents[2]
    return here

def daterange(dini: date, dfim: date):
    cur = dini
    while cur <= dfim:
        yield cur
        cur += timedelta(days=1)

def load_cidades(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Não encontrei lista de municípios: {path}")
    return pd.read_csv(path, sep=";")

def fetch_daily_archive(lat: float, lon: float, dia: date, tz: str) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": dia.strftime("%Y-%m-%d"),
        "end_date": dia.strftime("%Y-%m-%d"),
        "daily": (
            "temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_max,apparent_temperature_min,"
            "precipitation_sum,rain_sum,snowfall_sum,"
            "windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,"
            "shortwave_radiation_sum,weathercode"
        ),
        "timezone": tz,
    }
    r = requests.get("https://archive-api.open-meteo.com/v1/archive",
                     params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    js = r.json()
    return pd.DataFrame(js.get("daily", {}))

def fetch_hourly_archive(lat: float, lon: float, dia: date, tz: str) -> pd.DataFrame:
    # Tenta archive direto
    params_a = {
        "latitude": lat,
        "longitude": lon,
        "start_date": dia.strftime("%Y-%m-%d"),
        "end_date": dia.strftime("%Y-%m-%d"),
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": tz,
    }
    r = requests.get("https://archive-api.open-meteo.com/v1/archive",
                     params=params_a, timeout=HTTP_TIMEOUT)
    if r.status_code == 200:
        js = r.json()
        if js.get("hourly"):
            return pd.DataFrame(js["hourly"])

    # Fallback: forecast + filtro por data (raramente necessário pro histórico)
    params_f = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "past_days": 1,
        "timezone": tz,
    }
    r2 = requests.get("https://api.open-meteo.com/v1/forecast",
                      params=params_f, timeout=HTTP_TIMEOUT)
    r2.raise_for_status()
    df = pd.DataFrame(r2.json().get("hourly", {}))
    if "time" in df.columns:
        target = dia.strftime("%Y-%m-%d")
        df = df[df["time"].str.startswith(target)]
    return df

def save_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    # garante tipos básicos strings para evitar surpresa com pyarrow ao concatenar depois
    for col in df.columns:
        # Evita problemas com objetos mistos
        if df[col].dtype == "object":
            df[col] = df[col].astype("string")
    df.to_parquet(path, index=False, engine=PARQUET_ENGINE, compression=PARQUET_COMPRESSION)

def main():
    root = base_dir()
    path_lista = root / "data" / "lista_municipios" / "lista_mun.csv"
    # --- pastas separadas dentro de raw ---
    path_raw = root / "data" / "raw"
    path_raw_diario = path_raw / "diario"
    path_raw_horario = path_raw / "horario"

    df_cidades = load_cidades(path_lista)
    print(f"📦 Backfill one-off {DATA_INI} → {DATA_FIM} | cidades={len(df_cidades)}")
    print("BASE_DIR:", root)

    for dia in daterange(DATA_INI, DATA_FIM):
        dt_str = dia.strftime("%Y%m%d")
        print(f"\n=== {dt_str} ===")

        # ---------- Diário ----------
        dados_d, falhas_d = [], 0
        for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0], desc=f"DIARIO {dt_str}"):
            ok = False
            for attempt in range(RETRIES + 1):
                try:
                    dfd = fetch_daily_archive(float(row["latitude"]), float(row["longitude"]), dia, TIMEZONE)
                    if dfd is not None and not dfd.empty:
                        # reaproveita sua padronização diária
                        try:
                            payload = {"daily": dfd.to_dict(orient="list")}
                            dfdp = processar_clima(payload, row)
                        except Exception:
                            dfdp = dfd.copy()
                            dfdp["nome"] = row["nome"]
                            dfdp["nome_uf"] = row["nome_uf"]
                            dfdp["latitude"] = row["latitude"]
                            dfdp["longitude"] = row["longitude"]
                        if dfdp is not None and not dfdp.empty:
                            dados_d.append(dfdp)
                    ok = True
                    break
                except Exception as e:
                    if attempt >= RETRIES:
                        falhas_d += 1
                        print(f"Falha DIARIO {row.get('nome')} ({row.get('nome_uf')}): {e}")
                    else:
                        time.sleep(1)
            time.sleep(SLEEP_BETWEEN_CALLS)

        if dados_d:
            df_out_d = pd.concat(dados_d, ignore_index=True)
            out_d = path_raw_diario / f"dados_climaticos_diarios_{dt_str}.parquet"
            save_parquet(df_out_d, out_d)
            print(f"✅ Diário salvo (Parquet): {out_d}")
            if falhas_d:
                print(f"   Aviso: {falhas_d} município(s) falharam (DIÁRIO {dt_str})")
        else:
            print("⚠️ Diário vazio nesse dia.")

        # ---------- Horário ----------
        dados_h, falhas_h = [], 0
        for _, row in tqdm(df_cidades.iterrows(), total=df_cidades.shape[0], desc=f"HORARIO {dt_str}"):
            ok = False
            for attempt in range(RETRIES + 1):
                try:
                    dfh = fetch_hourly_archive(float(row["latitude"]), float(row["longitude"]), dia, TIMEZONE)
                    if dfh is not None and not dfh.empty:
                        # enriquecer + padronizar cabeçalho PT-BR e ordem
                        dfh["nome"] = row["nome"]
                        dfh["uf"] = row["nome_uf"]
                        dfh["latitude"] = row["latitude"]
                        dfh["longitude"] = row["longitude"]

                        dfh.rename(columns={
                            "time": "data_hora",
                            "temperature_2m": "temperatura_c",
                            "relative_humidity_2m": "umidade_relativa",
                            "precipitation": "precipitacao_mm",
                            "wind_speed_10m": "velocidade_vento_ms",
                            "nome": "municipio",
                        }, inplace=True)

                        ordered = [
                            "data_hora", "municipio", "uf", "latitude", "longitude",
                            "temperatura_c", "umidade_relativa", "precipitacao_mm", "velocidade_vento_ms",
                        ]
                        dfh = dfh[[c for c in ordered if c in dfh.columns]]

                        dados_h.append(dfh)
                    ok = True
                    break
                except Exception as e:
                    if attempt >= RETRIES:
                        falhas_h += 1
                        print(f"Falha HORARIO {row.get('nome')} ({row.get('nome_uf')}): {e}")
                    else:
                        time.sleep(1)
            time.sleep(SLEEP_BETWEEN_CALLS)

        if dados_h:
            df_out_h = pd.concat(dados_h, ignore_index=True)
            out_h = path_raw_horario / f"dados_climaticos_horarios_{dt_str}.parquet"
            save_parquet(df_out_h, out_h)
            print(f"✅ Horário salvo (Parquet): {out_h}")
            if falhas_h:
                print(f"   Aviso: {falhas_h} município(s) falharam (HORÁRIO {dt_str})")
        else:
            print("⚠️ Horário vazio nesse dia.")

    print("\n🎉 Backfill one-off concluído.")

if __name__ == "__main__":
    main()
