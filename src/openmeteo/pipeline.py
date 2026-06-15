"""Orquestração da coleta: datas pendentes, state file e execução por modo."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import pandas as pd
from dateutil.tz import gettz
from tqdm import tqdm

from .client import OpenMeteoClient
from .config import Settings
from .errors import OpenMeteoError
from .extractors import extract_daily, extract_hourly
from .schema import build_daily_df, build_hourly_df
from .storage import already_materialized, output_path, upload_to_s3, write_parquet_atomic

log = logging.getLogger(__name__)


# ---------------- datas / state ----------------
def today(settings: Settings) -> date:
    return datetime.now(gettz(settings.timezone)).date()


def d1(settings: Settings) -> date:
    return today(settings) - timedelta(days=1)


def load_last_run(settings: Settings) -> date | None:
    sf = settings.state_file
    if not sf.exists():
        return None
    try:
        return datetime.strptime(sf.read_text().strip(), "%Y-%m-%d").date()
    except ValueError:
        log.warning("state file inválido em %s; tratando como primeira execução", sf)
        return None


def save_last_run(settings: Settings, dia: date) -> None:
    sf = settings.state_file
    sf.parent.mkdir(parents=True, exist_ok=True)
    sf.write_text(dia.strftime("%Y-%m-%d"))


def pending_dates(settings: Settings) -> list[date]:
    last_run = load_last_run(settings)
    alvo = d1(settings)
    if last_run is None:
        return [alvo]
    dias, dia = [], last_run + timedelta(days=1)
    while dia <= alvo:
        dias.append(dia)
        dia += timedelta(days=1)
    return dias


# ---------------- coleta ----------------
def _load_cidades(settings: Settings) -> pd.DataFrame:
    return pd.read_csv(settings.municipios_csv, sep=";")


def collect_daily(settings: Settings, client: OpenMeteoClient, dia: date) -> pd.DataFrame | None:
    cidades = _load_cidades(settings)
    dt_str = dia.strftime("%Y-%m-%d")
    log.info("(DIÁRIO) coletando %s para %d municípios", dt_str, len(cidades))
    dados, falhas = [], 0
    for _, row in tqdm(cidades.iterrows(), total=cidades.shape[0], desc=f"DIARIO {dt_str}"):
        try:
            payload = extract_daily(client, settings, row["latitude"], row["longitude"], dt_str)
            dados.append(build_daily_df(payload, row))
        except OpenMeteoError as exc:
            falhas += 1
            log.warning("falha diário %s (%s): %s", row["nome"], row["nome_uf"], exc)
    if falhas:
        log.warning("(DIÁRIO) %s: %d município(s) falharam", dt_str, falhas)
    if not dados:
        return None
    return pd.concat(dados, ignore_index=True)


def collect_hourly(settings: Settings, client: OpenMeteoClient, dia: date) -> pd.DataFrame | None:
    cidades = _load_cidades(settings)
    dt_str = dia.strftime("%Y-%m-%d")
    log.info("(HORÁRIO) coletando %s para %d municípios", dt_str, len(cidades))
    dados, falhas = [], 0
    for _, row in tqdm(cidades.iterrows(), total=cidades.shape[0], desc=f"HORARIO {dt_str}"):
        try:
            raw_df, fonte = extract_hourly(client, settings, row["latitude"], row["longitude"], dt_str)
            dados.append(build_hourly_df(raw_df, row, fonte))
        except OpenMeteoError as exc:
            falhas += 1
            log.warning("falha horário %s (%s): %s", row["nome"], row["nome_uf"], exc)
    if falhas:
        log.warning("(HORÁRIO) %s: %d município(s) falharam", dt_str, falhas)
    if not dados:
        return None
    return pd.concat(dados, ignore_index=True)


_COLLECTORS = {"diario": collect_daily, "horario": collect_hourly}


def _materialize(settings: Settings, client: OpenMeteoClient, tipo: str, dia: date, *, force: bool) -> bool:
    destino = output_path(settings, tipo, dia)
    if already_materialized(destino) and not force:
        log.info("skip %s %s: já materializado (%s)", tipo, dia, destino.name)
        return False
    df = _COLLECTORS[tipo](settings, client, dia)
    if df is None:
        log.warning("nenhum dado %s coletado para %s", tipo, dia)
        return False
    write_parquet_atomic(df, destino)
    log.info("%s salvo: %s (%d linhas)", tipo, destino, len(df))
    if settings.s3_bucket:
        upload_to_s3(settings, destino, tipo, dia.strftime("%Y-%m-%d"))
    return True


def run(settings: Settings, modo: str = "ambos", *, force: bool = False,
        client: OpenMeteoClient | None = None) -> list[date]:
    """Coleta as datas pendentes para o(s) modo(s) indicado(s) e atualiza o state."""
    client = client or OpenMeteoClient(settings)
    datas = pending_dates(settings)
    if not datas:
        log.info("nenhuma data pendente; nada a fazer")
        return []

    log.info("datas a processar: %s", [str(d) for d in datas])
    tipos = ["diario", "horario"] if modo == "ambos" else [modo]

    for dia in datas:
        for tipo in tipos:
            _materialize(settings, client, tipo, dia, force=force)

    ultimo = max(datas)
    save_last_run(settings, ultimo)
    log.info("state atualizado para %s; processo concluído", ultimo)
    return datas
