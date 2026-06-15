# Dimensões de Qualidade

> **Propósito**: Vocabulário comum para especificar e medir qualidade de dados
> **Confiança**: 0.95
> **Validado**: 2026-06-10

## Visão geral

"Dado de qualidade" é vago; as seis dimensões tornam o requisito específico e mensurável.
Cada tabela importante deveria declarar quais dimensões importam e com que limite —
isso vira diretamente as expectations do pipeline e as métricas do monitoramento.

## O padrão

```python
# Da dimensão à métrica — exemplo de checagem programática (PySpark)
from pyspark.sql import functions as F

df = spark.table("silver.stations")
total = df.count()

metrics = {
    "completude_station_id": df.filter(F.col("station_id").isNotNull()).count() / total,
    "unicidade_station_id":  df.select("station_id").distinct().count() / total,
    "validade_uf":           df.filter(F.col("uf").isin(UFS_VALIDAS)).count() / total,
    "atualidade_horas":      df.agg(F.max("_ingested_at")).first()[0],  # vs SLA
}
# Persistir em tabela de métricas → série temporal de qualidade
```

## Referência rápida

| Dimensão | Onde validar (medalhão) |
|----------|------------------------|
| Completude, Validade | Bronze → Silver (expectations) |
| Unicidade | Silver (dedup é responsabilidade dela) |
| Consistência | Silver (conformação com referências) |
| Atualidade | Orquestração (monitorar chegada da fonte) |
| Acurácia | Processo periódico, fora do pipeline |

## Erros comuns

### Errado

```text
"Validar os dados" como tarefa única no fim do pipeline
→ tarde demais: o erro já contaminou joins e agregações.
```

### Correto

```text
Cada fronteira valida o que recebe; cada dimensão tem dono, limite e métrica.
Ex.: silver.stations exige unicidade=100% em station_id, validade_uf ≥ 99.9%.
```

## Relacionados

- [quarentena-rejeicao-alerta](quarentena-rejeicao-alerta.md)
- [expectativas-declarativas](../patterns/expectativas-declarativas.md)
