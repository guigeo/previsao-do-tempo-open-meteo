# Idempotência e Reprocessamento

> **Propósito**: Garantir que rodar a mesma carga duas vezes produz o mesmo resultado
> **Confiança**: 0.95
> **Validado**: 2026-06-10

## Visão geral

Pipelines falham e são re-executados — por retry automático, correção manual ou backfill.
Uma carga idempotente pode rodar N vezes sem duplicar nem corromper. As três estratégias
padrão: MERGE por chave, overwrite de partição/escopo, e streaming com checkpoint
(exactly-once por offset). Append puro só é idempotente se houver dedup a jusante.

## O padrão

```python
# Estratégia 1 — MERGE por chave natural (cargas incrementais)
# Reprocessar o mesmo arquivo: as mesmas chaves dão match → update, não insert.

# Estratégia 2 — overwrite do escopo da carga (snapshots periódicos)
(df.write.format("delta").mode("overwrite")
   .option("replaceWhere", "snapshot_date = '2026-06-01'")
   .saveAsTable("bronze.stations_snapshots"))
# Rodar de novo substitui exatamente a mesma fatia.

# Estratégia 3 — Auto Loader / streaming com checkpoint
# O checkpoint registra arquivos já processados; re-execução pula o que já foi lido.
```

## Referência rápida

| Tipo de carga | Estratégia idempotente |
|---------------|------------------------|
| Snapshot completo periódico | `overwrite` + `replaceWhere` da fatia |
| Incremental com updates | `MERGE` por chave natural |
| Arquivos chegando continuamente | Auto Loader com checkpoint |
| Append de eventos imutáveis | Append + dedup por id de evento na Silver |
| Fonte **append-only de grão único** → Silver | **Streaming Table** row-wise stateless: idempotência e incrementalidade nativas, sem MERGE (o checkpoint controla o que já foi lido) |

## Erros comuns

### Errado

```python
# Append cego de um snapshot periódico
df.write.mode("append").saveAsTable("bronze.entidades")
# Re-rodou o job? Período duplicado. Fonte atualizada? Linhas antigas e novas misturadas.
```

### Correto

```python
# Snapshot carimbado + substituição da própria fatia
(df.withColumn("snapshot_date", F.lit(run_date))
   .write.mode("overwrite").option("replaceWhere", f"snapshot_date = '{run_date}'")
   .saveAsTable("bronze.stations"))
```

## Relacionados

- [responsabilidades-das-camadas](responsabilidades-das-camadas.md)
- [contratos-e-nomenclatura](../patterns/contratos-e-nomenclatura.md)
