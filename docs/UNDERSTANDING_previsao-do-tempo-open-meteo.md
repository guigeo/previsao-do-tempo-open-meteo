# Relatório de Entendimento — previsao-do-tempo-open-meteo

> Gerado pelo `/adopt` em 2026-06-14 (adoção do projeto pelo template AgentSpec).
> Entendimento a partir da leitura do código (read-only). As oportunidades abaixo são
> uma **lista de candidatos**, não um plano priorizado — escolha uma como semente de `/brainstorm`.

## Executive Summary

Pipeline **end-to-end** de dados climáticos históricos (D-1) dos municípios do Brasil:

```
Open-Meteo API → Python/Docker (coleta diária+horária, state file, retry)
  → Parquet no S3 (/raw/clima/, Hive-style)
  → Databricks DLT medalhão: s3→Bronze → Silver → Gold (catalog open_meteo)
```

Arquitetura limpa e bem separada (ingestão desacoplada da transformação via S3). Stack:
Python 3.11 (pandas, requests, boto3, pyarrow, tqdm) + Docker; AWS S3; Databricks DLT em SQL.

## Deep Dive

### Ingestão (Python/Docker)
- `main.py` orquestra: resolve raiz, calcula D-1 (tz `America/Sao_Paulo`), lê/grava `state/last_run.txt`, itera municípios com `tqdm`.
- `src/recupera_dados_api_dia.py` / `_hora.py` — chamadas à Open-Meteo (requests).
- `src/processa_dados.py` — normalização/processamento (pandas).
- `src/upload_s3.py` — envio de Parquet ao S3 (boto3).
- `scripts/backfil_once.py` — backfill pontual.
- Empacotado em `python:3.11-slim` (Dockerfile + docker-compose, serviço `openmeteo`).

### Lakehouse (Databricks DLT)
- `open_meteo_s3_to_bronze` — `cloud_files()` + streaming live tables, schema raw (strings). Tabelas `clima_diario`, `clima_horario`.
- `open_meteo_bronze_to_silver` — tipos, dedupe, normalização.
- `open_meteo_silver_to_gold` — `clima_diario_historico` (1 linha por data × cidade, métricas derivadas, particionado por ano/mês).
- Catalog/schemas: `open_meteo.{bronze|silver|gold}`.

### Entidades
Município (`codigo_ibge`, `municipio`, `uf`, lat/long) × medições (temp, sensação, precipitação/chuva/neve, vento vel/rajada/direção, radiação, código WMO).

### Pontos fortes
- Boa separação de camadas; ingestão incremental com state file; `.gitignore` robusto; `.env` **não** versionado (segredos OK); medalhão idiomático em DLT.

## Oportunidades de melhoria (candidatos — não priorizados)

1. **Sem testes** — não há `tests/` nem pytest nas deps. Lógica pura (ex.: `processa_dados`) é candidata natural a testes unitários. (KB: qualidade-de-dados; agente: test-generator)
2. **Migrar para `uv`** — hoje `requirements.txt` com pins; não há `pyproject.toml`. Alinhar à convenção do template (uv, sem instalação global).
3. **Tratamento de exceção amplo** — `_carregar_last_run` usa `except:` nu; trocar por exceção específica (não engolir tudo).
4. **Expectations/qualidade na Silver** — Bronze é tudo string; reforçar validações declarativas (faixas plausíveis de temperatura/precipitação, dedupe por chaves naturais). (KB: qualidade-de-dados, databricks-lakeflow)
5. **Observabilidade da ingestão** — logging estruturado + métrica de "o que entrou, quando, status por município" (hoje há retry, mas pouca rastreabilidade).
6. **Modularização do `main.py`** — orquestração e helpers misturados; extrair camada de orquestração testável.
7. **Idempotência/reprocessamento** — formalizar a estratégia (state file + chaves naturais) à luz da KB arquitetura-medalhao (idempotência e reprocessamento).

## Próximos passos sugeridos
- Ler este relatório, escolher **uma** oportunidade e rodar `/brainstorm "<ela>"`.
- `/define` → `/design` → `/build` → `/ship`.
- Conhecimento genérico que surgir volta ao template via `/distill` → `/contribute`.
