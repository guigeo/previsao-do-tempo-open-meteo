# DEFINE: Refatoração da Camada de Extração (Open-Meteo → data/)

> Reorganizar a etapa de extração/gravação em um pacote `src/openmeteo/` coeso e testável, corrigindo fragilidades baratas, sem mudar o comportamento (exceto duas colunas novas no horário) e mantendo o runtime Docker funcionando.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | REFATORACAO_CAMADA_EXTRACAO |
| **Date** | 2026-06-14 |
| **Author** | define-agent |
| **Status** | Ready for Design |
| **Clarity Score** | 14/15 |

---

## Problem Statement

A camada de extração (Open-Meteo → `data/raw/`) tem a lógica espalhada entre `main.py` e `src/`, sem testes, com robustez assimétrica (o diário tem 5 retries; o horário **não tem retry**), gravação não-atômica (parquet pode corromper se falhar no meio), `except:` nu que engole bugs e observabilidade só por `print`. Para o mantenedor (solo), isso impede evoluir a ingestão com segurança e faz falhas de município virarem buracos silenciosos nos dados que chegam ao lakehouse.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Mantenedor | Dev solo do pipeline | Não consegue testar/evoluir a extração com confiança; lógica acoplada no `main.py`; falhas silenciosas difíceis de diagnosticar |
| Consumidor do lakehouse | Analista/consumidor da camada Gold | Risco de dados incompletos/corrompidos chegando à Bronze; horário sem `codigo_ibge` (join difícil) e sem proveniência (archive vs forecast) |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | Reempacotar a extração em `src/openmeteo/` (`client`, `extractors`, `schema`, `storage`, `pipeline`, `config`) com `main.py` como thin CLI |
| **MUST** | Cliente HTTP único com retry+backoff exponencial real e tratamento de `429`/`Retry-After`, aplicado a diário **e** horário |
| **MUST** | Gravação atômica (tmp+rename no mesmo volume) + skip idempotente quando o parquet do dia já existe |
| **MUST** | Validação de payload (erro estruturado/vazio) antes de gravar |
| **MUST** | Migrar tooling para uv (`pyproject.toml`/`uv.lock`) e atualizar o Dockerfile (pip→uv) na mesma feature |
| **MUST** | Suíte pytest com golden tests sobre os parquets/payloads existentes |
| **SHOULD** | Adicionar coluna `fonte` (archive\|forecast) e `codigo_ibge` ao horário; ajustar o DLT Bronze→Silver para o novo schema |
| **SHOULD** | Substituir `except:` nu e `print` por exceções específicas + logging estruturado em stdout |
| **COULD** | `scripts/backfil_once.py` passar a reusar o novo pacote (eliminar duplicação) |

---

## Success Criteria

- [ ] `src/openmeteo/` criado com 6 módulos; `main.py` ≤ ~40 linhas (só parse de args + chamada ao pipeline).
- [ ] 100% das chamadas HTTP (diário e horário) passam pelo mesmo `client` com retry (≥ 5 tentativas) e tratamento de `429`/`Retry-After`.
- [ ] 0 parquets parciais/corrompidos: escrita sempre via tmp no mesmo diretório de destino + rename atômico.
- [ ] Re-rodar uma data já materializada não dispara recoleta (skip idempotente verificável por ausência de requests).
- [ ] Cobertura de testes da lógica pura (parsing/normalização/validação/datas pendentes) ≥ 80%; suíte `uv run pytest` verde.
- [ ] Horário ganha exatamente 2 colunas novas (`fonte`, `codigo_ibge`) — diff de schema validado contra os golden parquets.
- [ ] `docker compose build` com uv conclui e `python main.py --modo ambos` roda no container sem erro de import/deps (smoke test).
- [ ] 0 ocorrências de `except:` nu; logs estruturados emitidos em stdout com status por execução (ok/falhas/linhas).

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Happy path D-1 | `lista_mun.csv` + state em D-2 | `python main.py --modo ambos` | Gera parquets diário/horário com schema esperado e atualiza `state/last_run.txt` para D-1 |
| AT-002 | Skip idempotente | Parquet do dia já existe em `data/raw/diario/` | Roda de novo para a mesma data | Não faz requests para essa data; arquivo existente intacto |
| AT-003 | Retry uniforme no horário | API responde `429` e depois `200` | Extrai horário | Respeita `Retry-After`, refaz e completa (mesmo comportamento do diário) |
| AT-004 | Gravação atômica | Escrita interrompida no meio | Processo morre antes do rename | Nenhum parquet parcial no destino; só resíduo `.tmp` no mesmo volume |
| AT-005 | Validação de payload | API responde `200` com `{"error": true}` ou `daily/hourly` vazio | Extrai a data | Levanta exceção específica e **não** grava arquivo |
| AT-006 | Behavior-preserving (golden) | Fixtures de payload conhecidos | Processa com o pacote refatorado | Parquet resultante bate com o golden, exceto as 2 colunas novas do horário |
| AT-007 | Proveniência do horário | Archive não tem o dia → cai no fallback forecast | Grava horário | Coluna `fonte = 'forecast'` (e `'archive'` no caminho normal) |
| AT-008 | Docker smoke | Imagem buildada com uv, volumes `state/` e `data/` montados | Container roda `python main.py --modo ambos` | Executa sem erro de import/deps e grava nos volumes |

---

## Out of Scope

- Batch de coordenadas / paralelismo (multi-lat/lon ou ThreadPool) — objetivo "escala", feature futura.
- Processar os 5.570 municípios (`lista_mun_tot.csv`) — hoje permanece em 101.
- Troca de stack: httpx/async e pydantic v2 — mantém `requests` + pandas.
- Retry automático por município faltante — por ora só registrar os faltantes (auditável).
- Mudanças nas camadas Silver/Gold além do ajuste mínimo do Bronze→Silver para as 2 colunas novas.

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Técnica | Manter `requests` + pandas (sem nova stack) | Refactor preserva comportamento; sem httpx/pydantic |
| Técnica | Preservar entrypoint `python main.py --modo {diario\|horario\|ambos}` | Scheduler do servidor depende da CLI; `main.py` vira thin wrapper |
| Técnica | Roda em **Docker num servidor** (job one-shot, `restart:"no"`); `state/` e `data/` são volumes | Dockerfile migra pip→uv; escrita atômica restrita ao volume `data/`; logs em stdout |
| Técnica | Escrita atômica só é atômica no mesmo filesystem | Arquivo `.tmp` precisa ficar dentro de `data/raw/...`, não em `/tmp` |
| Técnica | Schema do parquet horário muda (+2 colunas) | Exige ajuste do DLT `open_meteo_bronze_to_silver` na sequência |
| Contrato | Paths `data/raw/{diario,horario}/...` e formato do nome de arquivo mantidos | Upload S3 e DLT `cloud_files` continuam funcionando sem alteração de prefixo |
| Recurso | Projeto solo, sem orçamento de infra adicional | Sem novos serviços; reuso do que já existe |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `src/openmeteo/` (novo pacote); `main.py` thin CLI; `tests/`; raiz (`pyproject.toml`, `Dockerfile`) | Reorganização de `src/`; imports mudam |
| **KB Domains** | qualidade-de-dados (validação/quarentena), arquitetura-medalhao (idempotência/contratos), databricks-lakeflow (ajuste Bronze→Silver) | Padrões de validação, idempotência e contrato de schema |
| **IaC Impact** | None | Sem Terraform; mudança fica no Dockerfile/`docker-compose.yml` (já existem) |

**Why This Matters:**

- **Location** → garante que o novo pacote e os testes fiquem no lugar certo, com `main.py` preservando o contrato de execução.
- **KB Domains** → Design puxa padrões de validação de payload, idempotência e contrato de schema para o ajuste do DLT.
- **IaC Impact** → nenhuma infra nova; o acoplamento relevante é Dockerfile↔uv (a imagem do servidor precisa ser reconstruída).

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | O scheduler no servidor chama exatamente `python main.py --modo ambos` (entrypoint do container) | Se usar outro comando/flag, o thin CLI precisa expor a mesma superfície | [ ] |
| A-002 | Os parquets existentes em `data/raw/` representam o schema "correto" atual (servem de golden) | Golden tests validariam contra baseline errado | [x] |
| A-003 | Open-Meteo sinaliza rate-limit via HTTP `429` (+ `Retry-After` quando aplicável) | Tratamento de 429 não dispararia; retry genérico ainda cobre | [ ] |
| A-004 | Ajustar `open_meteo_bronze_to_silver` para +2 colunas é mudança aditiva (schema evolution) sem quebra | Poderia exigir full refresh da Bronze/Silver | [ ] |
| A-005 | `rename` dentro de `data/raw/...` (mesmo volume montado) é atômico no filesystem do servidor | Escrita atômica não garantiria atomicidade | [ ] |

**Note:** A-001, A-003 e A-005 valem confirmar no início do Design.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Pain points específicos e localizados no código (arquivos/linhas) |
| Users | 2 | Projeto solo; "consumidor do lakehouse" é indireto, mas pain points são reais |
| Goals | 3 | Priorizados MUST/SHOULD/COULD, mensuráveis |
| Success | 3 | Critérios com números/condições testáveis |
| Scope | 3 | Out-of-scope explícito e confirmado no brainstorm |
| **Total** | **14/15** | Acima do mínimo (12) |

---

## Open Questions

Nenhuma bloqueante para o Design. A confirmar no início do `/design`:
- Comando exato usado pelo scheduler do servidor (A-001).
- Estratégia de schema evolution no Bronze→Silver para as 2 colunas novas: aditivo vs full refresh (A-004).

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-14 | define-agent | Versão inicial a partir de BRAINSTORM_REFATORACAO_CAMADA_EXTRACAO.md |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_REFATORACAO_CAMADA_EXTRACAO.md`
