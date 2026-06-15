# Deduplicação por Chave Natural

> **Propósito**: Definir a chave de negócio de cada entidade e deduplicar de forma determinística
> **Validado**: 2026-06-10

## Quando usar

- Fontes que reenviam registros (snapshots, extrações repetidas)
- Mesma entidade aparecendo em múltiplas fontes ou múltiplas linhas (1 estação, N licenças)
- Antes de qualquer MERGE (fonte com chave duplicada aborta o MERGE)

## Implementação

```python
from pyspark.sql import functions as F, Window

# 1. Declarar a chave natural — decisão de modelagem, documentada
#    Ex.: ponto físico = (latitude, longitude, fonte) arredondadas,
#    ou o código oficial da estação quando a fonte fornece.
NATURAL_KEY = ["station_code"]

# 2. Dedup determinístico: definir QUAL duplicata sobrevive (não dropDuplicates cego)
w = Window.partitionBy(*NATURAL_KEY).orderBy(F.col("license_date").desc(),
                                             F.col("_ingested_at").desc())
dedup = (df.withColumn("_rn", F.row_number().over(w))
           .filter(F.col("_rn") == 1)
           .drop("_rn"))

# 3. Medir o que foi removido — dedup silencioso esconde problema na fonte
dups = df.count() - dedup.count()
if dups > 0:
    log.info(f"dedup removeu {dups} linhas por {NATURAL_KEY}")
```

## Configuração

| Decisão | Recomendação |
|---------|--------------|
| Critério de sobrevivência | Mais recente por data de negócio; desempate por `_ingested_at` |
| Chave natural instável | Criar surrogate, mas manter a natural para matching |
| Colunas float na chave | Arredondar explicitamente (`F.round(lat, 5)`) antes de comparar |
| Onde rodar | Silver — Bronze guarda as duplicatas (são evidência da fonte) |

## Exemplo de uso

```python
# Detectar (sem corrigir) chaves duplicadas — métrica de qualidade da fonte
dup_keys = (df.groupBy(*NATURAL_KEY).count().filter("count > 1"))
dup_keys.write.mode("overwrite").saveAsTable("monitor.stations_dup_keys")
```

## Ver também

- [expectativas-declarativas](expectativas-declarativas.md)
- [dimensoes-de-qualidade](../concepts/dimensoes-de-qualidade.md)
