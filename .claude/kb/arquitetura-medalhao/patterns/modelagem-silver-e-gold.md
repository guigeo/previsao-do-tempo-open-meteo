# Modelagem na Silver e na Gold

> **Propósito**: Decidir entre espelho 1:1 e modelo de domínio na Silver; desenhar a Gold pelo consumo
> **Validado**: 2026-06-10

## Quando usar

- Definindo as tabelas Silver de uma fonte nova
- A Silver 1:1 começou a exigir os mesmos joins repetidos em toda consulta
- Desenhando a Gold para um dashboard ou API específica

## Implementação

```text
SILVER — duas estratégias válidas:

1:1 (uma Silver por tabela de origem)         DOMÍNIO (entidade de negócio)
+ simples, lineage óbvio                       + consultas downstream simples
+ replay barato                                + conformação feita uma vez
- joins repetidos a jusante                    - pipeline mais complexo
→ padrão inicial                               → adotar quando 2+ consumidores
                                                 repetem o mesmo join

Regra prática: comece 1:1; promova para entidade de domínio quando o
mesmo join aparecer pela terceira vez em consumidores diferentes.

GOLD — desenhada pela pergunta, não pela origem:
- uma tabela por pergunta de negócio recorrente (dashboard, relatório, API)
- denormalizada: o consumidor não faz join
- grão explícito no nome e no comentário (ex.: _monthly, _by_city)
```

## Configuração

| Decisão | Padrão recomendado |
|---------|--------------------|
| Histórico de mudanças na Silver | SCD2 só se o negócio pergunta "como era antes"; senão SCD1 (+CDF) |
| Chave da Silver | chave natural da entidade; surrogate só se a natural for instável |
| Particionamento/clustering | pelas colunas mais filtradas pelo consumidor (não pela origem) |
| Agregações intermediárias | na Gold; a Silver não pré-agrega |

## Exemplo de uso

```text
Origem: arquivo de licenciamento (1 linha = 1 licença por estação/frequência)

silver.licenses        # 1:1 com a origem, limpa e tipada
silver.stations        # entidade derivada: 1 linha = 1 estação física (agrupa licenças)
gold.stations_by_city  # contagem/status por município — alimenta o mapa
gold.kpi_coverage_monthly  # série temporal para acompanhamento
```

## Ver também

- [contratos-e-nomenclatura](contratos-e-nomenclatura.md)
- [idempotencia-e-reprocessamento](../concepts/idempotencia-e-reprocessamento.md)
