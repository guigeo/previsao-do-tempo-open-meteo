"""Configuração central da camada de extração."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

TIMEZONE = "America/Sao_Paulo"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def _resolve_base_dir() -> Path:
    """Acha a raiz do projeto (diretório que contém data/lista_municipios)."""
    here = Path(__file__).resolve()
    for base in [*here.parents]:
        if (base / "data" / "lista_municipios").exists():
            return base
    return here.parents[2]


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    timezone: str = TIMEZONE
    archive_url: str = ARCHIVE_URL
    forecast_url: str = FORECAST_URL
    http_timeout: int = 60
    max_retries: int = 5
    backoff_base: float = 2.0
    backoff_max: float = 60.0
    s3_bucket: str | None = None

    @classmethod
    def load(cls, base_dir: Path | None = None) -> Settings:
        return cls(
            base_dir=base_dir or _resolve_base_dir(),
            s3_bucket=os.getenv("S3_BUCKET"),
        )

    # ---- paths derivados ----
    @property
    def municipios_csv(self) -> Path:
        return self.base_dir / "data" / "lista_municipios" / "lista_mun.csv"

    @property
    def state_file(self) -> Path:
        return self.base_dir / "state" / "last_run.txt"

    def raw_dir(self, tipo: str) -> Path:
        return self.base_dir / "data" / "raw" / tipo
