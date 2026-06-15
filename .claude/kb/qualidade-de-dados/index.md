# Qualidade de Dados — Domínio KB

> Validação, quarentena e monitoramento de qualidade em pipelines de dados.
> Domínio conceitual com exemplos em DLT expectations. Agnóstico de projeto.

## Quando carregar este domínio

- Definindo regras de validação para uma fonte nova (especialmente dados públicos/sujos)
- Decidindo o destino de linhas inválidas (derrubar? quarentenar? alertar?)
- Escolhendo chaves de deduplicação
- Criando métricas de qualidade para acompanhamento

## Arquivos

| Arquivo | O que responde |
|---------|----------------|
| [quick-reference.md](quick-reference.md) | Dimensões, matriz de severidade, anti-padrões |
| [concepts/dimensoes-de-qualidade.md](concepts/dimensoes-de-qualidade.md) | As 6 dimensões e como medi-las |
| [concepts/quarentena-rejeicao-alerta.md](concepts/quarentena-rejeicao-alerta.md) | Política de tratamento por severidade |
| [patterns/expectativas-declarativas.md](patterns/expectativas-declarativas.md) | Expectations DLT e checks programáticos |
| [patterns/deduplicacao-chaves-naturais.md](patterns/deduplicacao-chaves-naturais.md) | Identificar e deduplicar por chave de negócio |

## Domínios relacionados

- `arquitetura-medalhao/` — em qual fronteira cada validação roda
- `databricks-lakeflow/` — expectations nativas de pipeline
- `delta-lake/` — MERGE e constraints na camada de armazenamento
