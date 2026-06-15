"""Exceções específicas da camada de extração."""

from __future__ import annotations


class OpenMeteoError(Exception):
    """Erro base da camada de extração."""


class RequestFailedError(OpenMeteoError):
    """Falha de rede/HTTP após esgotar as tentativas."""


class RateLimitError(OpenMeteoError):
    """Rate limit persistente (429/503) após esgotar as tentativas."""


class PayloadError(OpenMeteoError):
    """Resposta 200 com erro estruturado ou payload vazio/inesperado."""
