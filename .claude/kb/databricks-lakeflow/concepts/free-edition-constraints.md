# Databricks Free Edition Constraints

> **Purpose**: Know the hard limits of Free Edition before designing pipelines for it
> **Confidence**: 0.95
> **MCP Validated**: 2026-06-10 (docs.databricks.com, page updated 2026-06-01)

## Overview

Databricks Free Edition is a no-cost workspace for learning and non-commercial use. It runs **serverless compute only** — no classic clusters, no custom compute configuration, no GPUs — and is subject to a fair-usage quota: exceed it and compute is shut down for the rest of the day (extreme cases: rest of the month). Data and settings are preserved; only compute pauses. No SLA or support coverage.

## The Pattern

Designing for Free Edition means designing for serverless and for small quotas:

```text
Free Edition design checklist
├── Compute: serverless notebooks / serverless pipelines only
├── Pipelines: ONE active declarative pipeline per pipeline type
├── Jobs: max 5 concurrent job tasks per account
├── SQL: one warehouse, fixed at 2X-Small
├── Languages: Python + SQL only (no R, no Scala)
├── Network: outbound internet restricted to trusted domains
└── Account: one workspace, one metastore, no account console/APIs
```

## Quick Reference

| Resource | Free Edition limit |
|----------|--------------------|
| Compute | Serverless only; limited size and usage |
| Lakeflow Declarative Pipelines | One active pipeline per pipeline type |
| Jobs | 5 concurrent job tasks per account |
| SQL warehouses | One, `2X-Small` |
| Model serving | Limited endpoints; no GPU serving, no batch inference on GPU |
| AI Search | One endpoint, one search unit |
| Apps | Up to 3; auto-stop after 24h running |
| Auth | Email OTP, Google, Microsoft sign-in; no SSO/SCIM |

Unsupported entirely: R and Scala, custom workspace storage locations, online tables, clean rooms, Lakebase database instances, legacy (non-UC) features, compliance/security customization, private networking. **Persisted `GEOMETRY`/`GEOGRAPHY` columns also fail** (`UNSUPPORTED_DATATYPE`) — for geo, persist `double` + H3 and use `ST_*` on-the-fly; see `geoespacial-databricks/`.

Design implications:

- **Serialize work**: with 5 concurrent tasks and one active pipeline, prefer one job that runs steps sequentially over parallel fan-out.
- **Keep datasets small**: fair-usage quota throttles heavy recomputes; favor incremental patterns (Auto Loader, streaming tables) over repeated full refreshes.
- **Triggered, not continuous**: continuous pipelines burn quota; use triggered updates on a schedule.
- **Everything is UC**: Free Edition is Unity Catalog-based — use volumes for files.
- **Not for production**: no SLA, no support, non-commercial use only; treat it as a prototyping target whose code ports cleanly to paid workspaces.

## Common Mistakes

### Wrong

```text
Plan: 3 always-on continuous pipelines + 10 parallel job tasks on Free Edition
```

### Correct

```text
Plan: 1 triggered pipeline (bronze→silver→gold in one DAG),
scheduled by 1 job, ≤5 concurrent tasks, serverless compute
```

### Ad-hoc serverless REPL is blocked

Running arbitrary code against serverless via the REPL channel (e.g. an `execute_code`-style API call) is rejected (`Client-1 channel` error). Run one-off conversions or exploration as a **notebook or a pipeline** (the native runtime works on serverless) — not as an API REPL session. Pipeline code (Auto Loader, streaming tables) runs fine; only the interactive REPL path is restricted.

## Related

- [unity-catalog-basics](../concepts/unity-catalog-basics.md)
- [jobs-orchestration](../patterns/jobs-orchestration.md)
