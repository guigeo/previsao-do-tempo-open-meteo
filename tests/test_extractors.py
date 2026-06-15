"""Testes de extração: validação de payload e proveniência archive/forecast."""

from __future__ import annotations

import pytest

from src.openmeteo.errors import PayloadError
from src.openmeteo.extractors import extract_daily, extract_hourly


class FakeClient:
    """Cliente que devolve respostas pré-programadas por URL, em ordem."""

    def __init__(self, settings, respostas: dict):
        self._s = settings
        self._respostas = {k: list(v) for k, v in respostas.items()}

    def get_json(self, url, params):
        return self._respostas[url].pop(0)


def test_extract_daily_ok(settings, daily_payload):
    client = FakeClient(settings, {settings.archive_url: [daily_payload]})
    out = extract_daily(client, settings, -23.5, -46.6, "2025-11-20")
    assert out["daily"]["time"] == ["2025-11-20"]


def test_extract_daily_erro_estruturado(settings):
    client = FakeClient(settings, {settings.archive_url: [{"error": True, "reason": "bad range"}]})
    with pytest.raises(PayloadError, match="bad range"):
        extract_daily(client, settings, -23.5, -46.6, "2025-11-20")


def test_extract_hourly_archive(settings, hourly_payload):
    client = FakeClient(settings, {settings.archive_url: [hourly_payload]})
    df, fonte = extract_hourly(client, settings, -23.5, -46.6, "2025-11-20")
    assert fonte == "archive"
    assert len(df) == 2


def test_extract_hourly_fallback_forecast(settings, hourly_payload):
    forecast = {
        "hourly": {
            "time": ["2025-11-19T23:00", "2025-11-20T00:00"],
            "temperature_2m": [18.0, 20.0],
            "relative_humidity_2m": [70, 75],
            "precipitation": [0.0, 0.0],
            "wind_speed_10m": [3.0, 4.0],
        }
    }
    client = FakeClient(settings, {
        settings.archive_url: [{"hourly": {}}],  # archive vazio → fallback
        settings.forecast_url: [forecast],
    })
    df, fonte = extract_hourly(client, settings, -23.5, -46.6, "2025-11-20")
    assert fonte == "forecast"
    assert (df["time"].str.startswith("2025-11-20")).all()
    assert len(df) == 1


def test_extract_hourly_tudo_vazio_levanta(settings):
    client = FakeClient(settings, {
        settings.archive_url: [{"hourly": {}}],
        settings.forecast_url: [{"hourly": {}}],
    })
    with pytest.raises(PayloadError):
        extract_hourly(client, settings, -23.5, -46.6, "2025-11-20")
