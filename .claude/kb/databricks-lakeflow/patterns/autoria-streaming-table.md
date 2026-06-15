# Autoria de Streaming Table (armadilhas)

> **Purpose**: Duas armadilhas práticas ao declarar Streaming Tables — uma pipeline com várias tabelas e comentários no schema
> **Validated**: 2026-06-13

## When to Use

- Vários estágios (ex.: bronze→silver) que precisam caber numa única pipeline
- Free Edition: limite de **1 pipeline ativa por tipo** força tudo no mesmo pipeline
- Streaming Table gerenciada que precisa de comentários de coluna documentados

## Implementation

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ARMADILHA 1 — Várias tabelas numa pipeline só, via nome QUALIFICADO de 3 níveis.
# O modo de publicação default do Lakeflow aceita catalog.schema.table; assim
# bronze e silver coexistem no mesmo pipeline (essencial na Free Edition).
BRONZE = "meu_catalogo.bronze.entidades"
SILVER = "meu_catalogo.silver.entidades"

# ARMADILHA 2 — COMMENT ON COLUMN é BLOQUEADO em Streaming Table gerenciada.
# Solução: declarar os comentários inline no `schema=` (DDL) do @dp.table,
# e o comentário da tabela em `comment=`. Nada de ALTER TABLE depois.
SCHEMA_DDL = (
    "id STRING COMMENT 'Identificador da entidade', "
    "valor DOUBLE COMMENT 'Métrica convertida de texto', "
    "_data_snapshot DATE COMMENT 'Data do snapshot'"
)

@dp.table(name=SILVER, comment="Entidades limpas (Silver).", schema=SCHEMA_DDL)
def silver_entidades():
    return spark.readStream.table(BRONZE).select("id", "valor", "_data_snapshot")
```

## Configuration

| Armadilha | Sintoma | Solução |
|-----------|---------|---------|
| Várias tabelas / 1 pipeline | Free Edition: 2ª pipeline não ativa | Nome qualificado 3 níveis no mesmo pipeline |
| `COMMENT ON COLUMN` | Erro em Streaming Table gerenciada | Comentários no `schema=` do `@dp.table` |
| Comentário da tabela | `COMMENT ON TABLE` idem | Usar `comment=` no decorator |

## Example Usage

```python
# Bronze e Silver no MESMO arquivo/pipeline — cada uma com seu @dp.table qualificado.
@dp.table(name="meu_catalogo.bronze.entidades")
def bronze_entidades(): ...
@dp.table(name="meu_catalogo.silver.entidades")
def silver_entidades(): ...
```

## See Also

- [free-edition-constraints](../concepts/free-edition-constraints.md)
- [jobs-orchestration](jobs-orchestration.md)
