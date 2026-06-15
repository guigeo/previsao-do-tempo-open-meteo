"""Testes do cliente HTTP: retry, 429/Retry-After, falha após esgotar tentativas."""

from __future__ import annotations

from dataclasses import replace

import pytest
import responses

from src.openmeteo.client import OpenMeteoClient
from src.openmeteo.errors import RateLimitError, RequestFailedError

URL = "https://archive-api.open-meteo.com/v1/archive"


@pytest.fixture
def fast_client(settings, monkeypatch):
    """Cliente com sleeps zerados e poucas tentativas para testes rápidos."""
    monkeypatch.setattr("src.openmeteo.client.time.sleep", lambda *_: None)
    return OpenMeteoClient(replace(settings, max_retries=3, backoff_base=0.0, backoff_max=0.0))


@responses.activate
def test_get_json_sucesso(fast_client):
    responses.add(responses.GET, URL, json={"daily": {"time": ["2025-11-20"]}}, status=200)
    assert fast_client.get_json(URL, {}) == {"daily": {"time": ["2025-11-20"]}}


@responses.activate
def test_get_json_retry_apos_500(fast_client):
    responses.add(responses.GET, URL, status=500)
    responses.add(responses.GET, URL, json={"ok": True}, status=200)
    assert fast_client.get_json(URL, {}) == {"ok": True}
    assert len(responses.calls) == 2


@responses.activate
def test_get_json_respeita_retry_after(fast_client, monkeypatch):
    vistos = []
    monkeypatch.setattr("src.openmeteo.client.time.sleep", lambda s: vistos.append(s))
    responses.add(responses.GET, URL, status=429, headers={"Retry-After": "7"})
    responses.add(responses.GET, URL, json={"ok": True}, status=200)
    assert fast_client.get_json(URL, {}) == {"ok": True}
    assert 7.0 in vistos


@responses.activate
def test_get_json_429_persistente_levanta_ratelimit(fast_client):
    responses.add(responses.GET, URL, status=429)
    with pytest.raises(RateLimitError):
        fast_client.get_json(URL, {})


@responses.activate
def test_get_json_falha_persistente_levanta(fast_client):
    responses.add(responses.GET, URL, status=500)
    with pytest.raises(RequestFailedError):
        fast_client.get_json(URL, {})
