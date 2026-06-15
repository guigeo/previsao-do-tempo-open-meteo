"""Cliente HTTP único para a Open-Meteo: retry, backoff exponencial e tratamento de 429."""

from __future__ import annotations

import logging
import random
import time

import requests

from .config import Settings
from .errors import RateLimitError, RequestFailedError

log = logging.getLogger(__name__)

_RATE_LIMIT_STATUS = (429, 503)


class OpenMeteoClient:
    """Encapsula chamadas GET com retry/backoff e respeito a Retry-After."""

    def __init__(self, settings: Settings, session: requests.Session | None = None):
        self._s = settings
        self._session = session or requests.Session()

    def get_json(self, url: str, params: dict) -> dict:
        last_exc: Exception | None = None

        for attempt in range(1, self._s.max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self._s.http_timeout)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt >= self._s.max_retries:
                    break
                wait = self._backoff(attempt)
                log.warning("erro de rede (tentativa %d/%d): %s; aguardando %.1fs",
                            attempt, self._s.max_retries, exc, wait)
                time.sleep(wait)
                continue

            if resp.status_code in _RATE_LIMIT_STATUS:
                if attempt >= self._s.max_retries:
                    raise RateLimitError(
                        f"rate limit {resp.status_code} persistente após {self._s.max_retries} tentativas"
                    )
                wait = self._retry_after(resp, attempt)
                log.warning("rate-limit %s (tentativa %d/%d); aguardando %.1fs",
                            resp.status_code, attempt, self._s.max_retries, wait)
                time.sleep(wait)
                continue

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                last_exc = exc
                if attempt >= self._s.max_retries:
                    break
                wait = self._backoff(attempt)
                log.warning("HTTP %s (tentativa %d/%d); aguardando %.1fs",
                            resp.status_code, attempt, self._s.max_retries, wait)
                time.sleep(wait)
                continue

            return resp.json()

        raise RequestFailedError(
            f"falha após {self._s.max_retries} tentativas: {last_exc}"
        ) from last_exc

    def _backoff(self, attempt: int) -> float:
        base = self._s.backoff_base * (2 ** (attempt - 1))
        return min(base, self._s.backoff_max) + random.uniform(0, 0.5)

    def _retry_after(self, resp: requests.Response, attempt: int) -> float:
        ra = resp.headers.get("Retry-After")
        if ra and ra.strip().isdigit():
            return float(ra)
        return self._backoff(attempt)
