# scripts/sync_s3.py
"""Sincroniza Parquets já materializados em data/raw/ para o S3, sem rechamar a API."""
from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import boto3  # noqa: E402

from src.openmeteo.config import Settings  # noqa: E402
from src.openmeteo.storage import upload_to_s3  # noqa: E402

log = logging.getLogger(__name__)

_DATE_RE = re.compile(r"(\d{8})\.parquet$")


def _parse(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _arquivos_locais(settings: Settings, tipo: str, ini: date | None, fim: date | None):
    for path in sorted(settings.raw_dir(tipo).glob("*.parquet")):
        m = _DATE_RE.search(path.name)
        if not m:
            continue
        dia = datetime.strptime(m.group(1), "%Y%m%d").date()
        if ini and dia < ini:
            continue
        if fim and dia > fim:
            continue
        yield dia, path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sincroniza Parquets locais (data/raw/) para o S3")
    p.add_argument("--ini", type=_parse, help="data inicial YYYY-MM-DD (default: todos os locais)")
    p.add_argument("--fim", type=_parse, help="data final YYYY-MM-DD (default: todos os locais)")
    p.add_argument("--modo", choices=["diario", "horario", "ambos"], default="ambos")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    args = parse_args()
    settings = Settings.load()
    if not settings.s3_bucket:
        raise SystemExit("S3_BUCKET não configurado no ambiente — abortando sync")

    client = boto3.client("s3")
    tipos = ["diario", "horario"] if args.modo == "ambos" else [args.modo]

    enviados, falhas = 0, 0
    for tipo in tipos:
        for dia, path in _arquivos_locais(settings, tipo, args.ini, args.fim):
            try:
                url = upload_to_s3(settings, path, tipo, dia.strftime("%Y-%m-%d"), client=client)
                log.info("sync ok: %s", url)
                enviados += 1
            except Exception as exc:  # mantém o lote rodando mesmo se um arquivo falhar
                log.error("sync falhou %s %s: %s", tipo, dia, exc)
                falhas += 1

    log.info("sync concluído: %d enviados, %d falhas", enviados, falhas)
    if falhas:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
