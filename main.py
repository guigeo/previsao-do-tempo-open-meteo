# main.py
from __future__ import annotations

import argparse
import logging

from src.openmeteo.config import Settings
from src.openmeteo.pipeline import run


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Coleta Open-Meteo – diário/horário/ambos (incremental, D-1)"
    )
    p.add_argument("--modo", choices=["diario", "horario", "ambos"], default="ambos")
    p.add_argument("--force", action="store_true",
                   help="recoleta mesmo se o parquet do dia já existir")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    args = parse_args()
    settings = Settings.load()
    logging.getLogger(__name__).info("BASE_DIR: %s", settings.base_dir)
    run(settings, modo=args.modo, force=args.force)


if __name__ == "__main__":
    main()
