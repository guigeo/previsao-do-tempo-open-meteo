# Expectativas Declarativas

> **Propósito**: Regras de qualidade declaradas junto à definição da tabela, não espalhadas no código
> **Validado**: 2026-06-10

## Quando usar

- Pipelines Lakeflow/DLT (expectations nativas)
- Qualquer tabela com regras de validade conhecidas
- Substituir `filter()`s de validação espalhados por declaração única

## Implementação

```python
# Lakeflow / DLT — expectations nativas (preferir quando disponível)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="silver_stations")
@dp.expect_or_drop("chave_valida", "station_id IS NOT NULL")          # rejeitar
@dp.expect("responsavel_preenchido", "responsavel IS NOT NULL")      # alertar (passa + métrica)
@dp.expect_or_fail("lote_integro", "_corrupt IS NULL")                # falhar pipeline
def silver_stations():
    return (dp.read("bronze_stations")
            .withColumn("uf", F.upper(F.trim("uf"))))

# Fora do DLT — mesmo espírito: regras numa estrutura, aplicação genérica
RULES = {
    "silver.stations": [
        ("chave_valida", "station_id IS NOT NULL", "drop"),
        ("uf_valida", "uf IN ('AC','AL','AM','AP','BA','CE','DF','ES','GO','MA',"
                      "'MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO',"
                      "'RR','RS','SC','SE','SP','TO')", "quarantine"),
    ]
}
```

## Configuração

| Decorator DLT | Linha reprovada | Uso |
|---------------|-----------------|-----|
| `@dp.expect` | Passa (métrica registrada) | Avisos |
| `@dp.expect_or_drop` | Removida (contada) | Regras críticas por linha |
| `@dp.expect_or_fail` | Aborta o update | Problemas estruturais de lote |
| `@dp.expect_all{_or_drop,_or_fail}` | dict de regras | Agrupar regras da tabela |

## Exemplo de uso

```sql
-- Métricas de expectations ficam no event log do pipeline:
SELECT timestamp, details:flow_progress.data_quality.expectations
FROM event_log(TABLE(silver_stations))
WHERE event_type = 'flow_progress';
```

## Ver também

- [quarentena-rejeicao-alerta](../concepts/quarentena-rejeicao-alerta.md)
- [deduplicacao-chaves-naturais](deduplicacao-chaves-naturais.md)
