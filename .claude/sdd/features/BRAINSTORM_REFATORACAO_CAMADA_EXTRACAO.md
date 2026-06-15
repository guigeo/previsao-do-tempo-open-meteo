# BRAINSTORM: Refatoração da Camada de Extração (Open-Meteo → data/)

> Sessão exploratória para clarificar intenção e abordagem antes da captura de requisitos

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | REFATORACAO_CAMADA_EXTRACAO |
| **Date** | 2026-06-14 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** "Quero melhorar o processo começando pelo início, a extração da API, ajustar o processo de extração e gravação dos dados na pasta `data`. Preciso de uma análise de como este passo está acontecendo hoje + sugestões de melhorias."

**Context Gathered:**
- A etapa de ingestão é desacoplada do lakehouse via S3; mexer nela não afeta o DLT diretamente (exceto se o schema do parquet mudar).
- Fluxo atual: `main.py` calcula D-1 a partir de `state/last_run.txt`, monta a lista de datas pendentes (backfill automático do gap), e para cada data itera **101 municípios** ([data/lista_municipios/lista_mun.csv](../../../data/lista_municipios/lista_mun.csv)) fazendo **1 request HTTP por município por endpoint, sequencial**.
- Existe [data/lista_municipios/lista_mun_tot.csv](../../../data/lista_municipios/lista_mun_tot.csv) com **5.570** municípios (Brasil inteiro) **não utilizada** hoje.
- Saída: 1 parquet/dia (todos municípios juntos) em `data/raw/{diario,horario}/dados_..._YYYYMMDD.parquet`, depois upload S3 e atualização do state.
- Já existem ~20 parquets de exemplo em `data/raw/` — servem de **golden fixtures** para testes.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `src/` → reempacotar em `src/openmeteo/`; `main.py` vira thin CLI | Novo pacote de extração coeso |
| Relevant KB Domains | qualidade-de-dados (validação de payload), arquitetura-medalhao (idempotência/contratos) | Padrões de validação e idempotência |
| IaC Patterns | N/A (Docker + S3 já existem; sem Terraform) | Sem mudança de infra |
| Tooling | Hoje `requirements.txt` (sem `pyproject.toml`/tests) | Migrar para uv + pytest nesta feature |
| Runtime/Deploy | Roda em **Docker num servidor** (job one-shot, `restart: "no"`); `state/` e `data/` montados como volumes; `CMD python main.py --modo ambos` | Dockerfile precisa migrar pip→uv; escrita atômica no mesmo volume; logging em stdout; entrypoint preservado |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Qual a dor principal que motiva mexer na extração? | **Refatorar / organizar** (camada de extração limpa e testável antes de novas features) | Foco em estrutura/testabilidade, não em escala ou robustez pesada |
| 2 | Até onde vai a refatoração? | **Reorganizar + corrigir fragilidades óbvias** (retry uniforme, gravação atômica, proveniência, validação) | Refactor não é 100% puro; inclui correções de baixo custo |
| 3 | Como tratar testes e tooling? | **Refatorar + uv + pytest juntos** | Migrar `requirements.txt`→`pyproject.toml`/uv e criar suíte pytest na mesma feature |
| 4 | Confirma Abordagem A com batch/escala fora do escopo? | **Sim, Abordagem A** | Pacote `src/openmeteo/` modular; batch/paralelismo adiado |
| 5 | Itens 6/7 mudam o schema do parquet (proveniência + codigo_ibge no horário). Incluir? | **Incluir ambas** | Schema raw muda; exige ajuste do DLT Bronze→Silver na sequência |

---

## Sample Data Inventory

> Samples improve accuracy through golden/regression testing of the refactor.

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input reference | `data/lista_municipios/lista_mun.csv` | 101 | `codigo_ibge;nome;nome_uf;latitude;longitude` (sep `;`) |
| Input reference (full) | `data/lista_municipios/lista_mun_tot.csv` | 5.570 | Brasil inteiro — não usado hoje (escopo futuro) |
| Output examples (diário) | `data/raw/diario/*.parquet` | ~20 | 1 arquivo/dia, todos municípios; ~19 KB cada |
| Output examples (horário) | `data/raw/horario/*.parquet` | ~20 | 1 arquivo/dia, todos municípios |
| Related code | `main.py`, `src/*.py`, `scripts/backfil_once.py` | 5 | Lógica a reorganizar; `backfil_once.py` duplica a extração |

**How samples will be used:**

- **Golden tests:** rodar o extrator refatorado e comparar o parquet gerado contra os existentes (campos que não mudam) → garante behavior-preserving onde se espera.
- **Fixtures de payload:** JSONs de exemplo da Open-Meteo (capturados) para testar parsing/validação sem rede.
- **Schema diff:** validar que apenas as colunas planejadas (`fonte`, `codigo_ibge` no horário) foram adicionadas.

---

## Approaches Explored

### Approach A: Pacote de extração orientado a domínio ⭐ Recomendada (SELECIONADA)

**Description:** Reempacotar `src/` num pacote coeso com responsabilidades isoladas:

```
src/openmeteo/
├── client.py     # 1 cliente HTTP: retry+backoff exponencial real, 429/Retry-After, timeout uniforme
├── extractors.py # daily/hourly: monta params, valida payload, marca proveniência (archive|forecast)
├── schema.py     # rename maps centralizados + chave natural unificada (codigo_ibge em ambos)
├── storage.py    # gravação atômica (tmp + rename) + skip idempotente
├── pipeline.py   # orquestração pura e testável (datas pendentes, loop, coleta)
└── config.py     # paths, timezone, lista de municípios, env
main.py           # thin CLI (parse args + chama pipeline)
tests/            # golden tests + fixtures; uv/pyproject.toml
```

**Pros:**
- Cada fragilidade cai na camada certa (retry no `client` beneficia diário **e** horário de graça).
- Orquestração testável de verdade (extraída do `main.py`).
- Base pronta para escala (interface de `client` permite batch/paralelismo depois).

**Cons:**
- Reorganização maior (imports mudam); PR mais largo.

**Why Recommended:** Ataca exatamente os 3 alvos escolhidos (organizar + corrigir o barato + testável) sem virar "remendo solto".

---

### Approach B: Refactor mínimo in-place

**Description:** Mantém os arquivos atuais; extrai só um `http_client` compartilhado, unifica schema num módulo e adiciona gravação atômica/validação.

**Pros:**
- PR menor, menos imports mexidos.

**Cons:**
- Orquestração continua presa no `main.py` — não entrega o "testável" prometido.

---

### Approach C: Reescrita com pydantic v2 + httpx

**Description:** Modela registros com pydantic; troca `requests`→`httpx` (retries/timeouts nativos).

**Pros:**
- Validação forte, stack moderna.

**Cons:**
- Nova dependência + maior superfície de mudança; mistura "refatorar" com "trocar stack" → risco maior num passo que deveria preservar comportamento.

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A |
| **User Confirmation** | 2026-06-14 |
| **Reasoning** | Entrega organização + correções baratas + testabilidade num pacote coeso; cada fragilidade resolvida na camada certa; base para escala futura sem implementá-la agora |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | Reempacotar em `src/openmeteo/`; `main.py` vira thin CLI | Isolar responsabilidades testáveis | Refactor mínimo in-place (B) |
| 2 | Cliente HTTP único com retry/backoff exponencial real + 429/Retry-After | Horário ganha retry de graça; backoff hoje é linear apesar do docstring | Manter retry só no diário |
| 3 | Gravação atômica (tmp + rename) + skip idempotente | Evita parquet corrompido e recoleta desnecessária | `to_parquet` direto no destino |
| 4 | Validação de payload antes de gravar (detecta `{"error":true}` / vazio) | Falha cedo, não grava lixo | Assumir `clima_json["daily"]` |
| 5 | Adicionar coluna `fonte` (archive\|forecast) no horário | Auditar proveniência (qualidades diferentes) | Misturar fontes sem marcação |
| 6 | Unificar chave natural: `codigo_ibge` também no horário | Joins consistentes diário↔horário | Horário só com municipio/uf |
| 7 | Migrar para uv + pytest nesta feature | CLAUDE.md exige; golden tests travam o refactor | Adiar tooling/tests |
| 8 | Trocar `except:` nu e prints por exceções específicas + logging estruturado | Não engolir bugs; rastreabilidade | Manter prints |
| 9 | Atualizar Dockerfile para uv junto da migração de tooling | Sem isso a imagem que roda no servidor quebra (deploy inconsistente) | Migrar uv sem tocar no Dockerfile |
| 10 | Escrita atômica usa arquivo temporário **dentro de `data/raw/...`** (mesmo volume) | `tmp+rename` só é atômico no mesmo filesystem; volume montado ≠ `/tmp` | Gravar em `/tmp` e renomear (vira cópia) |
| 11 | Logging estruturado vai para **stdout** (não arquivos) | Job one-shot em Docker; `PYTHONUNBUFFERED=1` já setado; servidor/Docker captura stdout | Escrever logs em arquivo |
| 12 | Preservar contrato do entrypoint `python main.py --modo {diario\|horario\|ambos}` | Scheduler do servidor depende dessa CLI | Mudar interface da CLI |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|-------------------|----------------|----------------|
| Batch de coordenadas / paralelismo (ThreadPool ou multi-lat/lon) | É o objetivo "escala", não "organizar". Interface fica preparada | Sim |
| Processar os 5.570 municípios (`lista_mun_tot.csv`) | Decisão de escopo de escala; fora desta feature | Sim |
| httpx / async | Troca de stack; risco num refactor que preserva comportamento | Sim |
| pydantic v2 para modelar registros | Validação leve resolve agora; pandas mantido | Sim |
| Retry automático por município faltante | É objetivo "robustez"; por ora só registrar faltantes (auditável) | Sim |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Diagnóstico do fluxo atual (tabela de 11 achados) | ✅ | Aceito como base | No |
| Foco (dor principal) | ✅ | Refatorar/organizar | No |
| Escopo do refactor | ✅ | Reorganizar + corrigir o barato | No |
| Tooling (uv/pytest) | ✅ | Incluir uv + pytest juntos | No |
| Abordagem (A vs B vs C) | ✅ | Abordagem A confirmada | No |
| Mudança de schema (itens 6/7) | ✅ | Incluir ambas; aceita ajustar DLT depois | No |
| Constraints de Docker/servidor | ✅ | Incluir Dockerfile→uv, escrita atômica no volume, logging stdout, entrypoint preservado | Yes (doc atualizado) |

---

## Suggested Requirements for /define

### Problem Statement (Draft)
A camada de extração (Open-Meteo → `data/raw/`) está com lógica espalhada entre `main.py` e `src/`, sem testes, com robustez assimétrica (horário sem retry), gravação não-atômica e tratamento de erro frágil — dificultando evolução segura.

### Target Users (Draft)
| User | Pain Point |
|------|------------|
| Mantenedor (solo, dev) | Não consegue testar/evoluir a extração com segurança; falhas de município são silenciosas |
| Consumidor do lakehouse | Risca de dados corrompidos/incompletos chegando à Bronze; sem proveniência no horário |

### Success Criteria (Draft)
- [ ] `src/openmeteo/` criado com `client`, `extractors`, `schema`, `storage`, `pipeline`, `config`; `main.py` thin CLI.
- [ ] Cliente HTTP único com retry+backoff exponencial real e tratamento de `429`/`Retry-After`, aplicado a diário **e** horário.
- [ ] Gravação atômica (tmp+rename) e skip idempotente quando o parquet do dia já existe.
- [ ] Validação de payload (erro estruturado / vazio) impede gravar lixo.
- [ ] Horário passa a ter coluna `fonte` (archive|forecast) e `codigo_ibge`.
- [ ] `except:` nu eliminado; logging estruturado no lugar dos `print`.
- [ ] `pyproject.toml` + uv configurados; suíte pytest com golden tests passando.
- [ ] DLT Bronze→Silver ajustado para o novo schema do horário.
- [ ] **Dockerfile migrado para uv** (`pyproject.toml`/`uv.lock`); imagem builda e o container roda `python main.py --modo ambos` (smoke test) sem regressão.
- [ ] Escrita atômica usa temporário no mesmo volume (`data/raw/...`); logging em stdout; entrypoint/CLI inalterados.

### Constraints Identified
- Manter `requests` + pandas (sem troca de stack).
- Comportamento preservado, exceto as duas colunas novas no horário (decisão explícita).
- Backfill/state file (`last_run.txt`) e contrato de paths em `data/raw/` mantidos.
- `scripts/backfil_once.py` deve passar a reusar o novo pacote (eliminar duplicação).
- **Roda em Docker num servidor** (job one-shot): Dockerfile migra pip→uv na mesma feature; escrita atômica restrita ao volume `data/`; logging em stdout; entrypoint `python main.py --modo ambos` preservado.

### Out of Scope (Confirmed)
- Batch de coordenadas / paralelismo.
- Escalar para os 5.570 municípios.
- httpx/async e pydantic.
- Retry automático por município faltante.

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 5 |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 5 |
| Validations Completed | 6 |
| Duration | ~1 sessão |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_REFATORACAO_CAMADA_EXTRACAO.md`
