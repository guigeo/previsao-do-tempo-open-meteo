# scripts/backfil_once.py
"""Backfill pontual de um intervalo de datas, reusando a camada src/openmeteo."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.openmeteo.client import OpenMeteoClient  # noqa: E402
from src.openmeteo.config import Settings  # noqa: E402
from src.openmeteo.pipeline import _materialize  # noqa: E402

log = logging.getLogger(__name__)


def _daterange(dini: date, dfim: date):
    cur = dini
    while cur <= dfim:
        yield cur
        cur += timedelta(days=1)


def _parse(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill Open-Meteo de um intervalo [ini, fim]")
    p.add_argument("--ini", required=True, type=_parse, help="data inicial YYYY-MM-DD")
    p.add_argument("--fim", required=True, type=_parse, help="data final YYYY-MM-DD")
    p.add_argument("--modo", choices=["diario", "horario", "ambos"], default="ambos")
    p.add_argument("--force", action="store_true")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    args = parse_args()
    settings = Settings.load()
    client = OpenMeteoClient(settings)
    tipos = ["diario", "horario"] if args.modo == "ambos" else [args.modo]

    log.info("backfill %s → %s | modo=%s", args.ini, args.fim, args.modo)
    for dia in _daterange(args.ini, args.fim):
        for tipo in tipos:
            _materialize(settings, client, tipo, dia, force=args.force)
    log.info("backfill concluído")


if __name__ == "__main__":
    main()
