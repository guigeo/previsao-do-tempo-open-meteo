# DESIGN: Refatoração da Camada de Extração (Open-Meteo → data/)

> Design técnico para reorganizar a extração/gravação em `src/openmeteo/`, com cliente HTTP robusto, gravação atômica idempotente, validação de payload, uv/Docker e testes — preservando o comportamento (exceto +2 colunas no horário).

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | REFATORACAO_CAMADA_EXTRACAO |
| **Date** | 2026-06-14 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_REFATORACAO_CAMADA_EXTRACAO.md](./DEFINE_REFATORACAO_CAMADA_EXTRACAO.md) |
| **Status** | Ready for Build |

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────┐
│                    CAMADA DE EXTRAÇÃO (src/openmeteo/)                  │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  main.py (thin CLI)                                                     │
│    │  parse --modo {diario|horario|ambos}                              │
│    ▼                                                                    │
│  pipeline.run(settings, modo)                                           │
│    │  pending_dates(state) ──► [datas]                                 │
│    │                                                                    │
│    │   para cada data, para cada município (lista_mun.csv):           │
│    │      ┌─────────────┐   ┌──────────────┐   ┌──────────┐          │
│    │      │ extractors  │──►│   client     │──►│ Open-Meteo│          │
│    │      │ daily/hourly│   │ retry/429    │   │  archive/ │          │
│    │      └──────┬──────┘   │ backoff exp  │   │  forecast │          │
│    │             │          └──────────────┘   └──────────┘          │
│    │             ▼ valida payload + marca fonte                       │
│    │      ┌─────────────┐                                             │
│    │      │   schema    │  rename + colunas município + codigo_ibge   │
│    │      └──────┬──────┘                                             │
│    │             ▼ DataFrame consolidado por dia                      │
│    │      ┌─────────────┐  tmp + os.replace (mesmo volume)            │
│    │      │  storage    │──► data/raw/{diario,horario}/*.parquet      │
│    │      │ atomic+skip │──► upload S3 (raw/clima/{tipo}/date=.../)    │
│    │      └─────────────┘                                             │
│    ▼                                                                    │
│  save_last_run(state)  ──► state/last_run.txt                          │
│                                                                        │
│  config.py (Settings) · errors.py (exceções) · logging → stdout       │
└──────────────────────────────────────────────────────────────────────┘
        │ (sem mudança de prefixo S3)
        ▼
   Databricks DLT: Bronze(+2 col) → Silver(+2 col) → Gold
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| `config.py` | `Settings` (frozen dataclass): paths, timezone, URLs, params de retry, `S3_BUCKET`, `municipios_csv`; resolve raiz do projeto | Python stdlib (dataclasses) |
| `errors.py` | Hierarquia de exceções específicas (`OpenMeteoError`, `PayloadError`, `RateLimitError`, `ExtractionError`) | Python stdlib |
| `client.py` | `OpenMeteoClient.get_json()` com retry+backoff exponencial real, trata `429`/`Retry-After`, timeout uniforme, sessão `requests` reutilizada | requests |
| `schema.py` | Mapas de rename (diário/horário), ordem de colunas, `build_daily_df`/`build_hourly_df` (inclui `codigo_ibge` e `fonte`) | pandas |
| `extractors.py` | `extract_daily`/`extract_hourly`: monta params, chama client, valida payload, define proveniência (archive\|forecast) | requests + pandas |
| `storage.py` | `write_parquet_atomic` (tmp no mesmo dir + `os.replace`), `output_path`, `already_materialized` (skip), `upload_to_s3` | pyarrow + boto3 |
| `pipeline.py` | Orquestração testável: `pending_dates`, `load_last_run`/`save_last_run`, `collect_daily`/`collect_hourly`, `run` | Python + pandas |
| `main.py` | Thin CLI: `argparse` → `pipeline.run` | argparse |

---

## Key Decisions

### Decision 1: Pacote de domínio com 8 módulos em `src/openmeteo/`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-14 |

**Context:** Hoje a lógica está acoplada no `main.py` (orquestração + I/O + schema) e duplicada em `scripts/backfil_once.py`.

**Choice:** Criar `src/openmeteo/` com `config`, `errors`, `client`, `schema`, `extractors`, `storage`, `pipeline`; `main.py` vira thin CLI; backfill reusa o pacote.

**Rationale:** Cada fragilidade cai na camada certa (retry no `client` cobre diário+horário); `pipeline` puro é testável; elimina a duplicação.

**Alternatives Rejected:**
1. Refactor mínimo in-place (B) — orquestração continua presa no `main.py`, não entrega "testável".
2. Reescrita com pydantic/httpx (C) — troca de stack, risco num refactor que preserva comportamento.

**Consequences:**
- (+) Testabilidade e separação claras; base para escala futura.
- (−) Imports mudam; PR mais largo.

---

### Decision 2: Cliente HTTP único com backoff exponencial real + `429`/`Retry-After`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-14 |

**Context:** Diário tem 5 retries lineares ("exponencial" só no docstring); horário não tem retry; timeouts divergentes (30s vs 60s).

**Choice:** `OpenMeteoClient.get_json()` centraliza retry com backoff exponencial + jitter, respeita `Retry-After` em `429`/`503`, timeout uniforme, `requests.Session` reutilizada.

**Rationale:** Uma só implementação robusta beneficia diário e horário; respeitar `Retry-After` evita ser bloqueado pela API.

**Alternatives Rejected:**
1. Manter retry só no diário — assimetria é a fragilidade #2 do DEFINE.
2. `urllib3.Retry` no adapter — menos controle sobre payload `200 {"error":true}` (que não é status de erro HTTP).

**Consequences:**
- (+) Robustez uniforme e configurável via `Settings`.
- (−) Erro estrutural com `200 OK` continua tratado fora do client (em `extractors`).

---

### Decision 3: Gravação atômica via `tmp` no mesmo diretório + `os.replace`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-14 |

**Context:** `to_parquet` direto no destino pode deixar arquivo parcial; roda em Docker com `data/` como volume montado.

**Choice:** Escrever em `…/.{nome}.tmp` no **mesmo diretório de destino** e `os.replace(tmp, final)` (atômico no mesmo filesystem).

**Rationale:** `os.replace` é atômico só dentro do mesmo filesystem; gravar em `/tmp` e mover cruzaria o limite do volume (vira cópia).

**Alternatives Rejected:**
1. `tempfile` em `/tmp` + `shutil.move` — não-atômico entre volume e rootfs.
2. Escrita direta — risco de parquet corrompido (fragilidade #5).

**Consequences:**
- (+) Nunca há parquet parcial no destino.
- (−) Pode sobrar `.tmp` se o processo morrer; limpeza no início do `write`.

---

### Decision 4: Skip idempotente por arquivo já materializado

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-14 |

**Context:** Re-rodar uma data recoleta tudo de novo (fragilidade #6).

**Choice:** Antes de coletar, se `output_path(data, tipo)` já existe e é não-vazio, pular a coleta (configurável via flag `--force` para sobrescrever).

**Rationale:** Idempotência barata sem estado extra; `--force` cobre reprocessamento intencional.

**Alternatives Rejected:**
1. Hash/manifest de controle — overhead desnecessário para o volume atual.

**Consequences:**
- (+) Re-execução não dispara requests à toa (AT-002).
- (−) Skip é por dia inteiro, não por município (consistente com "buraco" ser tratado depois — fora de escopo).

---

### Decision 5: `fonte` + `codigo_ibge` no horário e evolução aditiva no DLT

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-14 |

**Context:** Horário não tem `codigo_ibge` (chave natural divergente do diário) nem marca proveniência archive/forecast.

**Choice:** `build_hourly_df` adiciona `codigo_ibge` (do CSV) e `fonte` (`'archive'`|`'forecast'`). No DLT: adicionar as 2 colunas ao `SELECT` do Bronze e do Silver horário. Evolução **aditiva** (Bronze já usa `cloudFiles.schemaEvolutionMode=addNewColumns`).

**Rationale:** Chave consistente p/ joins diário↔horário; auditoria de qualidade da fonte. Aditivo evita full refresh.

**Alternatives Rejected:**
1. Não mexer no schema — perde join e auditoria (decisão do usuário foi "incluir ambas").
2. Full refresh da Bronze/Silver — custo desnecessário; mudança é aditiva.

**Consequences:**
- (+) Schema mais correto e auditável; sem reprocessar histórico.
- (−) Registros antigos ficam com `fonte`/`codigo_ibge` nulos (esperado; documentar).

---

### Decision 6: Migrar tooling para uv e Dockerfile pip→uv

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-14 |

**Context:** Hoje `requirements.txt`+pip; CLAUDE.md exige uv; imagem roda no servidor.

**Choice:** `pyproject.toml` + `uv.lock`; Dockerfile usa `ghcr.io/astral-sh/uv` (copia `pyproject.toml`/`uv.lock`, `uv sync --frozen --no-dev`); `requirements.txt` removido. Entrypoint `python main.py --modo ambos` preservado via `uv run` ou venv no PATH.

**Rationale:** Migrar uv sem ajustar o Dockerfile quebraria o deploy (acoplamento obrigatório).

**Alternatives Rejected:**
1. uv só local, Dockerfile com pip — imagem e dev divergem.
2. Adiar uv — contraria o objetivo da feature.

**Consequences:**
- (+) Ambiente reprodutível (lockfile) local e no container.
- (−) Rebuild da imagem necessário; `.dockerignore` para não copiar `data/`, `tests/`, `.venv`.

---

### Decision 7: Logging estruturado para stdout (stdlib `logging`)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-14 |

**Context:** Hoje só `print`; job one-shot em Docker (`PYTHONUNBUFFERED=1`).

**Choice:** `logging` com handler em stdout; uma linha de resumo por execução (data, tipo, municípios ok/falha, linhas, duração); manter `tqdm` para barra de progresso.

**Rationale:** stdout é capturado pelo Docker/servidor; sem arquivos de log; troca o `except:` nu por exceções específicas logadas.

**Alternatives Rejected:**
1. Arquivos de log no volume — desnecessário; Docker já agrega stdout.

**Consequences:**
- (+) Rastreabilidade do "o que entrou, quando, status".
- (−) Sem agregação central (fora de escopo; objetivo "observabilidade" futuro).

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `src/openmeteo/__init__.py` | Create | Marca o pacote + versão | @python-developer | None |
| 2 | `src/openmeteo/errors.py` | Create | Hierarquia de exceções específicas | @python-developer | None |
| 3 | `src/openmeteo/config.py` | Create | `Settings` (paths, tz, URLs, retry, env) + resolve raiz | @python-developer | None |
| 4 | `src/openmeteo/client.py` | Create | Cliente HTTP: retry/backoff/429/Retry-After | @python-developer | 2, 3 |
| 5 | `src/openmeteo/schema.py` | Create | Rename maps, ordem de colunas, builders (+`codigo_ibge`/`fonte`) | @pipeline-developer | None |
| 6 | `src/openmeteo/extractors.py` | Create | `extract_daily`/`extract_hourly` + validação payload + proveniência | @pipeline-developer | 2,4,5 |
| 7 | `src/openmeteo/storage.py` | Create | Escrita atômica, skip idempotente, upload S3 | @python-developer | 3 |
| 8 | `src/openmeteo/pipeline.py` | Create | Orquestração: pending_dates, state, collect, run | @pipeline-developer | 3,6,7 |
| 9 | `main.py` | Modify | Thin CLI → `pipeline.run` (preserva `--modo`, +`--force`) | @python-developer | 8 |
| 10 | `scripts/backfil_once.py` | Modify | Reusar o pacote (elimina duplicação) | @pipeline-developer | 8 |
| 11 | `pyproject.toml` | Create | Deps (uv), config ruff/pytest, pacote | @python-developer | None |
| 12 | `Dockerfile` | Modify | pip→uv (`uv sync --frozen`); preserva CMD | @python-developer | 11 |
| 13 | `.dockerignore` | Create | Excluir `data/`, `tests/`, `.venv`, `state/` | @python-developer | None |
| 14 | `tests/conftest.py` | Create | Fixtures: payloads JSON, row município, tmp dirs | @test-generator | 1-8 |
| 15 | `tests/fixtures/` | Create | Payloads archive/forecast + golden parquet | @test-generator | None |
| 16 | `tests/test_schema.py` | Create | Builders diário/horário, colunas novas, ordem | @test-generator | 5 |
| 17 | `tests/test_client.py` | Create | Retry, 429/Retry-After, timeout (requests mockado) | @test-generator | 4 |
| 18 | `tests/test_storage.py` | Create | Atomicidade (tmp+replace), skip idempotente | @test-generator | 7 |
| 19 | `tests/test_pipeline.py` | Create | pending_dates, state, run (extractors mockados) | @test-generator | 8 |
| 20 | `tests/test_extractors.py` | Create | Validação payload `{"error":true}`/vazio, fonte | @test-generator | 6 |
| 21 | `databricks/.../get_s3_to_bronze_hora.sql` | Modify | +`codigo_ibge`,`fonte` (CAST STRING) no SELECT | @lakeflow-expert | None |
| 22 | `databricks/.../get_bronze_to_silver_hora.sql` | Modify | +`codigo_ibge`,`fonte` tipados/normalizados | @lakeflow-expert | 21 |

**Total Files:** 22 (13 create, 9 modify-ish — 5 modify + 17 create contando fixtures como 1)

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|----------------|----------------|
| @python-developer | 1,2,3,4,7,9,11,12,13 | Estrutura de pacote, cliente HTTP, dataclasses, atomic I/O, uv/Docker |
| @pipeline-developer | 5,6,8,10 | Schema/transformação de domínio climático, orquestração, backfill |
| @test-generator | 14-20 | pytest, fixtures, mocks de `requests`, golden tests |
| @lakeflow-expert | 21,22 | Ajuste de DLT SQL (Bronze→Silver), schema evolution |

**Agent Discovery:** scaneado `.claude/agents/**/*.md`; match por tipo de arquivo, palavras-chave (HTTP/pacote → python; DLT/SQL → lakeflow; schema climático → pipeline-developer; testes → test-generator).

---

## Code Patterns

### Pattern 1: Cliente HTTP com retry/backoff e 429

```python
# src/openmeteo/client.py
from __future__ import annotations
import logging, random, time
import requests
from .config import Settings
from .errors import RateLimitError, OpenMeteoError

log = logging.getLogger(__name__)

class OpenMeteoClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None):
        self._s = settings
        self._session = session or requests.Session()

    def get_json(self, url: str, params: dict) -> dict:
        last_exc: Exception | None = None
        for attempt in range(1, self._s.max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self._s.http_timeout)
                if resp.status_code in (429, 503):
                    wait = self._retry_after(resp, attempt)
                    log.warning("rate-limit %s; aguardando %.1fs (tentativa %d)", resp.status_code, wait, attempt)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self._s.max_retries:
                    break
                time.sleep(self._backoff(attempt))
        raise OpenMeteoError(f"falha após {self._s.max_retries} tentativas: {last_exc}") from last_exc

    def _backoff(self, attempt: int) -> float:
        base = self._s.backoff_base * (2 ** (attempt - 1))   # exponencial real
        return min(base, self._s.backoff_max) + random.uniform(0, 0.5)  # + jitter

    def _retry_after(self, resp: requests.Response, attempt: int) -> float:
        ra = resp.headers.get("Retry-After")
        if ra and ra.isdigit():
            return float(ra)
        return self._backoff(attempt)
```

### Pattern 2: Validação de payload + proveniência (extractors)

```python
# src/openmeteo/extractors.py
def extract_hourly(client, settings, lat, lon, dia_str) -> tuple[pd.DataFrame, str]:
    js = client.get_json(settings.archive_url, _hourly_params(lat, lon, dia_str, settings.timezone))
    _raise_on_api_error(js)                      # 200 {"error": true, "reason": ...}
    if js.get("hourly"):
        return pd.DataFrame(js["hourly"]), "archive"
    # fallback forecast (marca fonte)
    jf = client.get_json(settings.forecast_url, _forecast_params(lat, lon, settings.timezone))
    _raise_on_api_error(jf)
    df = pd.DataFrame(jf.get("hourly", {}))
    if "time" in df.columns:
        df = df[df["time"].str.startswith(dia_str)]
    if df.empty:
        raise PayloadError(f"sem dados horários para {dia_str} ({lat},{lon})")
    return df, "forecast"

def _raise_on_api_error(js: dict) -> None:
    if isinstance(js, dict) and js.get("error"):
        raise PayloadError(js.get("reason", "erro estruturado da API"))
```

### Pattern 3: Gravação atômica no mesmo volume

```python
# src/openmeteo/storage.py
import os
from pathlib import Path
import pandas as pd

def write_parquet_atomic(df: pd.DataFrame, final_path: Path, *, compression="snappy") -> Path:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = final_path.with_name(f".{final_path.name}.tmp")
    if tmp.exists():
        tmp.unlink()
    df.to_parquet(tmp, index=False, engine="pyarrow", compression=compression)
    os.replace(tmp, final_path)        # atômico no mesmo filesystem (volume data/)
    return final_path

def already_materialized(final_path: Path) -> bool:
    return final_path.exists() and final_path.stat().st_size > 0
```

### Pattern 4: Settings (configuração)

```python
# src/openmeteo/config.py
from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class Settings:
    base_dir: Path
    timezone: str = "America/Sao_Paulo"
    archive_url: str = "https://archive-api.open-meteo.com/v1/archive"
    forecast_url: str = "https://api.open-meteo.com/v1/forecast"
    http_timeout: int = 60
    max_retries: int = 5
    backoff_base: float = 2.0
    backoff_max: float = 60.0
    s3_bucket: str | None = None

    @classmethod
    def load(cls) -> "Settings":
        base = _resolve_base_dir()
        return cls(base_dir=base, s3_bucket=os.getenv("S3_BUCKET"))
```

### Pattern 5: DLT Bronze horário (+2 colunas)

```sql
-- get_s3_to_bronze_hora.sql (trecho a adicionar no SELECT)
    CAST(codigo_ibge AS STRING)         AS codigo_ibge,
    CAST(fonte AS STRING)               AS fonte,
```

### Pattern 6: Dockerfile com uv

```dockerfile
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY . .
ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "main.py", "--modo", "ambos"]
```

---

## Data Flow

```text
1. main.py parseia --modo (+ --force) e chama pipeline.run(settings, modo)
   │
   ▼
2. pending_dates(): lê state/last_run.txt → lista de datas (last_run+1 … D-1); 1ª exec = [D-1]
   │
   ▼
3. para cada data × cada município (lista_mun.csv):
   extractors → client (retry/429) → Open-Meteo (archive; fallback forecast p/ horário)
   │   valida payload (sem {"error":true}, não-vazio); define fonte
   ▼
4. schema: rename + colunas do município (codigo_ibge em ambos) + fonte (horário)
   │   concat por dia → DataFrame consolidado
   ▼
5. storage: se já materializado e sem --force → skip; senão write_parquet_atomic (tmp+replace)
   │   upload_to_s3 (raw/clima/{tipo}/date=YYYY-MM-DD/…)
   ▼
6. save_last_run(max(datas)) → state/last_run.txt
   │
   ▼
7. (downstream, inalterado) DLT: Bronze(+2col) → Silver(+2col) → Gold
```

---

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-----------------|----------------|
| Open-Meteo Archive API | REST GET (requests) | Nenhuma (pública) |
| Open-Meteo Forecast API | REST GET (fallback horário) | Nenhuma (pública) |
| AWS S3 | SDK (boto3 `upload_file`) | Credenciais do ambiente (`.env`/IAM); `S3_BUCKET` |
| Databricks DLT | Consumidor downstream via S3 (`cloud_files`) | Desacoplado — só contrato de schema/prefixo |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|---------------|
| Unit | schema builders, validação, datas pendentes, atomicidade/skip | test_schema, test_extractors, test_storage, test_pipeline | pytest | ≥ 80% lógica pura |
| Unit (mock) | retry/429/Retry-After/timeout | test_client | pytest + responses/monkeypatch | Caminhos de retry |
| Golden/regressão | parquet refatorado vs baseline existente (exceto +2 col) | test_schema/test_pipeline | pytest + fixtures `data/raw` | Behavior-preserving |
| Integração (manual) | `uv run python main.py --modo ambos` numa data | — | uv | Happy path (AT-001) |
| Smoke (Docker) | `docker compose build` + `run` | — | docker | AT-008 |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|-------------------|--------|
| HTTP timeout / `RequestException` | Backoff exponencial + jitter no client; falha após `max_retries` → `OpenMeteoError` | Sim |
| `429`/`503` rate-limit | Respeita `Retry-After`; senão backoff | Sim |
| `200 OK` com `{"error": true}` | `PayloadError` em `extractors`; não grava | Não |
| Payload vazio (`daily/hourly` ausente) | `PayloadError`; município contabilizado como falha, segue os demais | Não |
| Falha de 1 município | Loga warning + incrementa contador; **não** aborta o dia | Não (escopo futuro) |
| Falha na escrita | `tmp` descartado, destino intacto; exceção propaga | Não |
| `S3_BUCKET` ausente / erro boto3 | Loga erro; parquet local permanece (re-upload possível) | Não |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `timezone` | str | `America/Sao_Paulo` | TZ para D-1 e params da API |
| `http_timeout` | int | `60` | Timeout uniforme (diário+horário) |
| `max_retries` | int | `5` | Tentativas no client |
| `backoff_base` | float | `2.0` | Base do backoff exponencial (s) |
| `backoff_max` | float | `60.0` | Teto do backoff (s) |
| `s3_bucket` | str | `env S3_BUCKET` | Bucket de destino |
| `--modo` (CLI) | enum | `ambos` | `diario\|horario\|ambos` (preservado) |
| `--force` (CLI) | flag | `false` | Sobrescreve mesmo se já materializado |

---

## Security Considerations

- Credenciais AWS via ambiente/`.env` (já não versionado); nada hardcoded — manter.
- `.dockerignore` evita copiar `.env`, `data/`, `state/` para a imagem.
- Open-Meteo é pública; sem segredos nas URLs/params.
- Logs em stdout não devem imprimir credenciais (boto3 não loga por padrão).

---

## Observability

| Aspect | Implementation |
|--------|----------------|
| Logging | stdlib `logging` → stdout (JSON/linha estruturada); resumo por execução: data, tipo, municípios ok/falha, linhas, duração |
| Progress | `tqdm` mantido por loop de municípios |
| Métricas | Contadores de falha por tipo/dia no log (sem backend externo — futuro) |
| Tracing | N/A nesta feature |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-14 | design-agent | Versão inicial a partir do DEFINE |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_REFATORACAO_CAMADA_EXTRACAO.md`
