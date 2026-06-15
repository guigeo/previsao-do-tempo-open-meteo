# Quarentena, Rejeição e Alerta

> **Propósito**: Política explícita para o destino de cada linha inválida
> **Confiança**: 0.95
> **Validado**: 2026-06-10

## Visão geral

Toda regra de validação precisa de uma decisão de tratamento: a linha reprovada é
descartada (rejeição), desviada para análise (quarentena) ou passa com registro (alerta)?
Sem política explícita, o padrão de fato vira "descarte silencioso" — e perda silenciosa
de dados é o defeito mais caro de diagnosticar num pipeline.

## O padrão

```python
from pyspark.sql import functions as F

regras = (
    (F.col("station_id").isNotNull(), "rejeitar"),     # crítica: sem chave, sem linha
    (F.col("latitude").between(-34, 6), "quarentenar"), # grave: coordenada suspeita (BR)
    (F.col("operator").isNotNull(), "alertar"),         # aviso: passa, mas conta
)

df = spark.table("bronze.stations")
validas     = df.filter(regras[0][0] & regras[1][0])
rejeitadas  = df.filter(~regras[0][0])
quarentena  = df.filter(regras[0][0] & ~regras[1][0])

validas.write.mode("append").saveAsTable("silver.stations")
quarentena.withColumn("_motivo", F.lit("coordenada_fora_br")) \
    .write.mode("append").saveAsTable("silver.stations_quarantine")
# rejeitadas: apenas contar e logar — volume anormal = investigar a fonte
```

## Referência rápida

| Tratamento | Quando | Obrigação |
|------------|--------|-----------|
| Rejeitar | Linha inutilizável (chave nula, lixo) | Contar + logar volume |
| Quarentenar | Suspeita mas potencialmente recuperável | Tabela própria + `_motivo` + revisão periódica |
| Alertar | Degrada mas não invalida | Métrica com threshold de alerta |
| Falhar pipeline | Lote estruturalmente ruim (>X% reprovado) | Circuit breaker — não carregar nada |

## Erros comuns

### Errado

```text
Quarentena criada e nunca mais olhada → vira cemitério; pior que rejeitar.
```

### Correto

```text
Quarentena com SLA: métrica de volume alerta se > limite; rotina mensal decide
recuperar (corrigir e reinserir) ou expirar (descartar com registro).
```

## Relacionados

- [dimensoes-de-qualidade](dimensoes-de-qualidade.md)
- [expectativas-declarativas](../patterns/expectativas-declarativas.md)
