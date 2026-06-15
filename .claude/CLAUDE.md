# previsao-do-tempo-open-meteo

> Pipeline end-to-end de dados climáticos (D-1) dos municípios do Brasil: Open-Meteo API → Python/Docker → S3 → Databricks Lakehouse (Medalhão).

---

## Contexto do Projeto

**Problema:** Coletar dados meteorológicos históricos (diários e horários) de todos os municípios brasileiros de forma automatizada, robusta e reprocessável, e disponibilizá-los como lakehouse analítico.

**Solução:** Ingestão em Python (containerizada em Docker) consome a Open-Meteo API para o dia anterior (D-1), salva Parquet particionado no S3, e um pipeline Databricks DLT (Lakeflow) transforma os dados em camadas Bronze → Silver → Gold (catalog `open_meteo`).

**Stack:** Python 3.11 (pandas, requests, boto3, pyarrow, tqdm) + Docker · AWS S3 · Databricks DLT (SQL), arquitetura Medalhão.

**Equipe:** Solo.

---

## Visão Geral da Arquitetura

```
Open-Meteo API (D-1) → Python/Docker (main.py: coleta diária+horária, state file, retry)
  → Parquet no S3 (/raw/clima/, Hive-style)
  → Databricks DLT medalhão:
       open_meteo_s3_to_bronze   (cloud_files + streaming tables, raw string)
       open_meteo_bronze_to_silver (tipos, dedupe, normalização)
       open_meteo_silver_to_gold   (clima_diario_historico, métricas derivadas, part. ano/mês)
```

> Rode `/sync-context` para aprofundar esta seção a partir do código.

---

## Estrutura do Projeto

```text
previsao-do-tempo-open-meteo/
├── main.py                  # Orquestração D-1 (diário+horário, state file)
├── src/                     # recupera_dados_api_*, processa_dados, upload_s3
├── scripts/                 # backfil_once.py
├── databricks/pipeline_dlt/ # DLT SQL: s3→bronze, bronze→silver, silver→gold
├── data/                    # lista_municipios, raw
├── state/                   # last_run.txt (controle incremental)
├── docs/                    # UNDERSTANDING_*.md (adoção)
├── Dockerfile, docker-compose.yml, requirements.txt
└── .claude/                 # AgentSpec/SDD (adotado via /adopt)
```

---

## Workflows de Desenvolvimento

### AgentSpec 4.2 (Spec-Driven Development)

```text
/brainstorm → /define → /design → /build → /ship
  (Opus)      (Opus)    (Opus)   (Sonnet)  (Haiku)
```

| Comando | Fase | Propósito |
|---------|------|-----------|
| `/brainstorm` | 0 | Explorar ideias (opcional) |
| `/define` | 1 | Capturar e validar requisitos |
| `/design` | 2 | Criar arquitetura e especificação |
| `/build` | 3 | Executar implementação |
| `/ship` | 4 | Arquivar com lições aprendidas |
| `/iterate` | Qualquer | Atualizar documentos mid-stream |

**Artefatos:** `.claude/sdd/features/` e `.claude/sdd/archive/`

> **Projeto adotado via `/adopt`** — veja `docs/UNDERSTANDING_previsao-do-tempo-open-meteo.md` para o entendimento do código e as oportunidades de melhoria. Escolha uma como semente de `/brainstorm`.

### Dev Loop (Nível 2 Agentico)

```bash
/dev "Quero construir X"              # O crafter te guia
/dev tasks/PROMPT_FEATURE.md          # Executa PROMPT existente
/dev tasks/PROMPT_FEATURE.md --resume # Retoma sessão interrompida
```

---

## Diretrizes de Uso de Agentes

| Categoria | Agentes | Quando usar |
|-----------|---------|-------------|
| **Workflow** | brainstorm, define, design, build, ship, iterate | Construir features com SDD |
| **Qualidade** | code-reviewer, code-documenter, code-cleaner, python-developer, test-generator | Revisar e melhorar código Python |
| **Engenharia de dados** | medallion-architect, lakeflow-architect, lakeflow-expert, lakeflow-pipeline-builder, ai-data-engineer | Pipelines DLT, medalhão, modelagem |
| **Exploração** | codebase-explorer, kb-architect | Explorar repositório, criar KBs |
| **Comunicação** | adaptive-explainer, meeting-analyst, the-planner | Explicações, planejamento |
| **Domínio** | previsao-do-tempo-open-meteo-expert, pipeline-developer | Decisões de domínio/negócio e transformações de dados |

---

## Padrões de Código

### Linguagem: Python 3.11

- **Style:** Ruff
- **Testes:** pytest
- **Validação:** Pydantic v2 (quando aplicável)
- **Type Hints:** Obrigatórios em todas as assinaturas de função

> **Projetos Python — obrigatório:** usar **uv** (não pip/venv). Comandos: `uv init`,
> `uv add <pkg>`, `uv run <cmd>`, `uv sync`; ferramentas one-off com `uvx <ferramenta>`.
> **Nunca instalar pacotes globalmente na máquina** — tudo isolado no ambiente do projeto.

---

## Knowledge Base

| Domínio | Propósito | Ponto de entrada |
|---------|-----------|-----------------|
| arquitetura-medalhao | Camadas Bronze/Silver/Gold, contratos, idempotência | `.claude/kb/arquitetura-medalhao/index.md` |
| databricks-lakeflow | DLT/Spark Declarative Pipelines, expectations, Free Edition | `.claude/kb/databricks-lakeflow/index.md` |
| delta-lake | ACID, MERGE/upsert, schema evolution, manutenção | `.claude/kb/delta-lake/index.md` |
| qualidade-de-dados | Dimensões, quarentena, expectations, deduplicação | `.claude/kb/qualidade-de-dados/index.md` |

Adicione domínios com `/create-kb "<dominio>"`.

---

## Features Ativas (Em Progresso)

| Feature | Status | Descrição |
|---------|--------|-----------|
| — | — | — |

---

## Features Entregues (Arquivo SDD)

| Feature | Entregue em | Descrição |
|---------|-------------|-----------|
| — | — | — |

---

## Ajuda

- **Workflow SDD:** [.claude/sdd/_index.md](.claude/sdd/_index.md)
- **Dev Loop:** [.claude/dev/_index.md](.claude/dev/_index.md)
- **Agentes:** [.claude/agents/](.claude/agents/)
- **KB Index:** [.claude/kb/_index.yaml](.claude/kb/_index.yaml)
- **Entendimento do projeto (adoção):** [docs/UNDERSTANDING_previsao-do-tempo-open-meteo.md](docs/UNDERSTANDING_previsao-do-tempo-open-meteo.md)
