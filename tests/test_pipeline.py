"""Testes de orquestração: datas pendentes, state e run (extractors mockados)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.openmeteo import pipeline
from src.openmeteo.storage import output_path


def test_pending_dates_primeira_execucao(settings, monkeypatch):
    monkeypatch.setattr(pipeline, "d1", lambda s: date(2025, 11, 20))
    assert pipeline.pending_dates(settings) == [date(2025, 11, 20)]


def test_pending_dates_preenche_gap(settings, monkeypatch):
    pipeline.save_last_run(settings, date(2025, 11, 18))
    monkeypatch.setattr(pipeline, "d1", lambda s: date(2025, 11, 20))
    assert pipeline.pending_dates(settings) == [date(2025, 11, 19), date(2025, 11, 20)]


def test_load_last_run_invalido_retorna_none(settings):
    settings.state_file.write_text("não-é-data")
    assert pipeline.load_last_run(settings) is None


def _patch_extractors(monkeypatch, daily_payload, hourly_payload):
    monkeypatch.setattr(pipeline, "extract_daily", lambda *a, **k: daily_payload)
    monkeypatch.setattr(pipeline, "extract_hourly",
                        lambda *a, **k: (pd.DataFrame(hourly_payload["hourly"]), "archive"))


def test_run_materializa_e_atualiza_state(settings, monkeypatch, daily_payload, hourly_payload):
    monkeypatch.setattr(pipeline, "d1", lambda s: date(2025, 11, 20))
    _patch_extractors(monkeypatch, daily_payload, hourly_payload)

    datas = pipeline.run(settings, modo="ambos", client=object())

    assert datas == [date(2025, 11, 20)]
    assert output_path(settings, "diario", date(2025, 11, 20)).exists()
    assert output_path(settings, "horario", date(2025, 11, 20)).exists()
    assert pipeline.load_last_run(settings) == date(2025, 11, 20)


def test_run_skip_idempotente(settings, monkeypatch, daily_payload, hourly_payload):
    monkeypatch.setattr(pipeline, "d1", lambda s: date(2025, 11, 20))

    chamadas = {"n": 0}

    def conta_daily(*a, **k):
        chamadas["n"] += 1
        return daily_payload

    monkeypatch.setattr(pipeline, "extract_daily", conta_daily)
    monkeypatch.setattr(pipeline, "extract_hourly",
                        lambda *a, **k: (pd.DataFrame(hourly_payload["hourly"]), "archive"))

    pipeline.run(settings, modo="diario", client=object())
    assert chamadas["n"] == 1

    # segunda execução para a mesma data: deve pular (sem novas chamadas)
    settings.state_file.write_text("2025-11-19")  # reabre a data como pendente
    pipeline.run(settings, modo="diario", client=object())
    assert chamadas["n"] == 1  # não recoletou


def test_run_force_recoleta(settings, monkeypatch, daily_payload, hourly_payload):
    monkeypatch.setattr(pipeline, "d1", lambda s: date(2025, 11, 20))
    chamadas = {"n": 0}

    def conta_daily(*a, **k):
        chamadas["n"] += 1
        return daily_payload

    monkeypatch.setattr(pipeline, "extract_daily", conta_daily)
    pipeline.run(settings, modo="diario", client=object())
    settings.state_file.write_text("2025-11-19")
    pipeline.run(settings, modo="diario", force=True, client=object())
    assert chamadas["n"] == 2
