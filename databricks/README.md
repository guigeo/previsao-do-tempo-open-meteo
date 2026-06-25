# Pipeline Databricks — Arquitetura Medalhão

> Atualizado em 2026-06-25 a partir do estado real do workspace (não é mais um template genérico).

## 📌 Visão Geral

Pipeline **Spark Declarative Pipelines / Lakeflow (SQL)** que processa dados climáticos em arquitetura **Medalhão** (Bronze → Silver → Gold) no catálogo `open_meteo`. Os dados são ingeridos do **S3** (enviados pela aplicação Python/Docker, ver raiz do repo) e o pipeline dispara **automaticamente quando chegam arquivos novos** no prefixo S3 — não é cron, é trigger por chegada de arquivo.

### Fluxo de Dados

```
┌──────────────────────────────────────────┐
│  S3 Raw (Parquet)                        │  ← Python/Docker escreve aqui
│  s3://gbrj-open-meteo-datalake/raw/clima/│
└──────────────────┬────────────────────────┘
                   │  Job "job_pipeline_openmeteo" dispara via
                   │  trigger file_arrival nesse prefixo
                   ▼
┌─────────────────────────────────────────────────┐
│  BRONZE (open_meteo.bronze)                     │
│  - cloud_files() / Auto Loader, schema string    │
│  - Tabelas: clima_dia_dia, clima_hora_hora       │
└──────────────────┬──────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────┐
│  SILVER (open_meteo.silver)                     │
│  - Tipagem, de-para UF, expectations de          │
│    qualidade (CONSTRAINT/EXPECT)                 │
│  - Tabelas: clima_dia_dia, clima_hora_hora       │
└──────────────────┬──────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────┐
│  GOLD (open_meteo.gold)                         │
│  - Agregações prontas para o dashboard           │
│  - Ver inventário de tabelas abaixo (algumas      │
│    estão órfãs — não fazem parte do pipeline)    │
└─────────────────────────────────────────────────┘
```

## 🗂️ Estrutura de Diretórios (real)

```
databricks/
├── README.md
├── dashboard/
│   └── Dash OpenMeteo.lvdash.json     # usa só as tabelas Gold "ativas" (ver inventário)
└── pipeline/
    └── pipeline_openmeteo/
        ├── gold_metricas.sql           # ⚠️ ÓRFÃO — fora do glob do pipeline, ver abaixo
        └── transformations/
            ├── raw_to_bronze_diario.sql
            ├── raw_to_bronze_horario.sql
            ├── bronze_to_silver_diario.sql
            ├── bronze_to_silver_horario.sql
            ├── gold_media_temp_chuva.sql
            ├── gold_min_max_temp_dias.sql
            └── gold_min_max_temp_muns.sql
```

O pipeline no workspace (`pipeline_openmeteo`, id `68827b61-cedc-4f21-b1f2-1f2f26771ce1`) só inclui arquivos via glob:

```
/Repos/<usuário>/previsao-do-tempo-open-meteo/databricks/pipeline/pipeline_openmeteo/transformations/**
```

**`gold_metricas.sql` está um nível acima de `transformations/`, então nunca entra no DAG.** As 4 tabelas que ele define (`clima_dia_historico`, `clima_extremo`, `clima_tendencia`, `clima_hora_analitico`) foram criadas manualmente uma vez (~2025-12-04) e nunca mais foram atualizadas — congeladas. Decisão tomada em 2026-06-25: não mexer agora, decidir o destino delas junto do redesign do dashboard (manter e mover pro glob, ou descontinuar).

## 📊 Inventário de Tabelas

| Camada | Tabela | Status |
|---|---|---|
| bronze | `clima_dia_dia`, `clima_hora_hora` | ✅ ativas, atualizadas a cada chegada de arquivo |
| silver | `clima_dia_dia`, `clima_hora_hora` | ✅ ativas, com expectations de qualidade |
| gold | `temp_max_dias_top10`, `temp_mins_dias_top10`, `temp_max_muns_top10`, `temp_mins_muns_top10`, `media_temp_chuva_muns` | ✅ ativas, usadas pelo dashboard |
| gold | `clima_dia_historico`, `clima_extremo`, `clima_tendencia`, `clima_hora_analitico` | ⚠️ **órfãs**, congeladas desde 2025-12-04, decisão pendente |

## ⚙️ Deploy e Sincronização (sem IaC)

**Não há `databricks.yml`/DAB neste projeto.** O código é servido via **Databricks Repos** (`/Repos/<usuário>/previsao-do-tempo-open-meteo`), que é um clone git que **não sincroniza automaticamente** com pushes no GitHub. Depois de qualquer commit que toque `databricks/`, é preciso atualizar o Repo manualmente:

```bash
databricks repos update <repo_id> --branch main --profile "My Free Edition"
```

> O profile `DEFAULT` do `~/.databrickscfg` está com um PAT expirado — use sempre `"My Free Edition"` nos comandos `databricks repos *`.

Para checar se o Repo está atrasado em relação ao git antes de confiar no pipeline:

```bash
databricks repos get <repo_id> --profile "My Free Edition"   # head_commit_id
git rev-parse origin/main                                     # compara
git log <head_commit_id>..origin/main --oneline -- databricks/  # o que ficou de fora
```

Esse drift já causou um incidente real: o Repo ficou ~6 commits atrasado e a Silver horária rodou meses sem as colunas `codigo_ibge`/`fonte` sem que ninguém notasse (corrigido em 2026-06-25).

## 🤖 Automação

- **Pipeline:** `pipeline_openmeteo` (`68827b61-cedc-4f21-b1f2-1f2f26771ce1`), serverless, target catalog `open_meteo`.
- **Job:** `job_pipeline_openmeteo` (`1067507991558724`) — dispara o pipeline via **trigger `file_arrival`** no prefixo `s3://gbrj-open-meteo-datalake/raw/clima/`. Não é agendamento por horário: qualquer Parquet novo nesse prefixo (vindo do cron na VM, ver raiz do repo) acorda o job automaticamente.
- Notificações de sucesso/falha do pipeline e do job vão por e-mail para o usuário dono do workspace.

Para rodar manualmente (ex: depois de mudar uma transformação):

```python
# via MCP/SDK: manage_pipeline_run(action="start", pipeline_id=..., wait=True)
# full refresh seletivo (nomes ambíguos entre camadas — sempre qualificar):
# full_refresh_selection=["open_meteo.silver.clima_hora_hora"]
```

## 🧪 Qualidade de Dados

A Silver (`bronze_to_silver_diario.sql`, `bronze_to_silver_horario.sql`) tem `CONSTRAINT ... EXPECT` declarativas (ver KB `qualidade-de-dados`):

- **`ON VIOLATION DROP ROW`** para valores fisicamente impossíveis: coordenada fora do Brasil, umidade fora de 0–100, vento/precipitação negativos, `codigo_ibge` nulo no diário.
- **Sem `ON VIOLATION`** (warn, métrica registrada, linha mantida) para extremos plausíveis mas suspeitos (temperatura fora de -15..50°C) e para `codigo_ibge` nulo no horário — esse último por causa do gap legado abaixo.

Os campos numéricos (`temp_max_c`, `chuva_mm`, `vento_velocidade_max_kmh` etc.) **não usam mais `COALESCE(..., 0)`**: leitura ausente fica `NULL`, não vira "zero" — isso evita que `AVG`/`MIN`/`MAX` no Gold confundam "sem dado" com "valor real zero".

### Gap conhecido: `codigo_ibge`/`fonte` no horário legado

Registros horários com `data_hora < 2026-03-06` têm `codigo_ibge` e `fonte` `NULL` (~297k de 560k linhas) — o Parquet de origem no S3 daquele período veio de uma versão do extrator Python sem essas colunas. Decisão (2026-06-25): manter como gap documentado, sem backfill nem reprocessamento. Dados diários não têm esse problema (sempre completos desde 2025-11-01).

## 📐 Escopo de Municípios

A coleta cobre hoje **100 municípios** (piloto, `data/lista_municipios/lista_mun.csv`), não os ~5.570 do Brasil (`lista_mun_tot.csv`, já presente no repo mas não usado). Expansão nacional é uma decisão separada e ainda não tomada — implica ~55x mais volume e pode estourar os limites da conta Databricks Free Edition.

## 🔗 Integração com Python

O Python/Docker (raiz do repo) envia Parquets pro S3; o trigger `file_arrival` do job dispara o pipeline automaticamente.

**Prefixo S3 real:**
```
s3://gbrj-open-meteo-datalake/raw/clima/diario/date=2026-06-24/dados_climaticos_diarios_20260624.parquet
s3://gbrj-open-meteo-datalake/raw/clima/horario/date=2026-06-24/dados_climaticos_horarios_20260624.parquet
```

## 🐛 Troubleshooting

### Pipeline não reflete uma mudança recente no SQL

**Causa:** Repo do Databricks desatualizado (não há auto-sync com git).

**Solução:** `databricks repos update <repo_id> --branch main --profile "My Free Edition"`, depois rodar o pipeline (full refresh se a mudança afetar dados já materializados, ex: trocar `COALESCE` ou adicionar coluna).

### `full_refresh_selection` não pega a tabela certa

**Causa:** nome de tabela ambíguo entre camadas (ex: `clima_hora_hora` existe em bronze e silver).

**Solução:** qualificar sempre com `catalog.schema.tabela`, ex: `open_meteo.silver.clima_hora_hora`.

### "Path does not exist" no `cloud_files()`

**Causa:** path S3 incorreto ou sem dados nesse prefixo.

**Solução:** confirmar `input_path_diario`/`input_path_horario` na config do pipeline e checar no console AWS se há Parquet no prefixo.

## 📚 Referências

- [Lakeflow / Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/etl-patterns-at-scale-with-medallion-architecture-and-databricks.html)
- [Cloud Files (Auto Loader)](https://docs.databricks.com/en/ingestion/cloud-object-storage/index.html)
- KB interna: `.claude/kb/arquitetura-medalhao/`, `.claude/kb/databricks-lakeflow/`, `.claude/kb/qualidade-de-dados/`
