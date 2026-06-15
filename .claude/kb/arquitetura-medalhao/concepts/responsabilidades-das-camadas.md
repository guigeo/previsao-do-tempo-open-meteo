# Responsabilidades das Camadas

> **Propósito**: Definir o que pertence (e o que não pertence) a Bronze, Silver e Gold
> **Confiança**: 0.95
> **Validado**: 2026-06-10

## Visão geral

A arquitetura medalhão progride o dado em qualidade a cada camada. O valor do padrão
está na disciplina das fronteiras: cada camada tem um contrato claro, e violar a
responsabilidade de uma camada (limpar na Bronze, regra de negócio na Silver difusa)
destrói a auditabilidade e o replay que justificam o padrão.

## O padrão

```text
Origem (CSV/XLS/API)
   │  ingestão fiel — sem transformação
   ▼
BRONZE  espelho do dado de origem + _ingested_at, _source_file
   │  limpeza: tipos, encoding, dedup, conformação de referência
   ▼
SILVER  entidades limpas e únicas — "uma linha = um fato verdadeiro"
   │  regra de negócio: agregação, KPI, denormalização para consumo
   ▼
GOLD    visões prontas para BI/API — otimizadas para a pergunta, não para a origem
```

## Referência rápida

| Pergunta | Resposta |
|----------|----------|
| Posso filtrar linhas inválidas na Bronze? | Não — marque/quarentene na Silver |
| Posso ter mais de uma Silver da mesma Bronze? | Sim — conformações diferentes por uso |
| Gold pode juntar várias Silvers? | Sim — é o lugar para isso |
| Onde fica o schema enforcement? | Fronteira Bronze→Silver (mais rígido) |

## Erros comuns

### Errado

```text
Bronze já tipada e deduplicada "para economizar uma camada"
→ origem mudou de formato? Você perdeu o registro do que ela enviava antes.
```

### Correto

```text
Bronze cru (string em tudo, se necessário) + Silver tipada
→ replay completo possível: re-derive a Silver inteira a partir da Bronze.
```

## Relacionados

- [idempotencia-e-reprocessamento](idempotencia-e-reprocessamento.md)
- [modelagem-silver-e-gold](../patterns/modelagem-silver-e-gold.md)
