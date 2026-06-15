# Arquitetura Medalhão — Domínio KB

> Conhecimento geral das camadas Bronze/Silver/Gold em lakehouse.
> Domínio conceitual, agnóstico de projeto e de ferramenta.

## Quando carregar este domínio

- Desenhar um pipeline novo ou revisar a separação de camadas
- Decidir em qual camada uma transformação pertence
- Definir nomenclatura de tabelas e contratos entre camadas
- Planejar reprocessamento sem duplicar dados

## Arquivos

| Arquivo | O que responde |
|---------|----------------|
| [quick-reference.md](quick-reference.md) | Tabela-resumo por camada, matriz de decisão, anti-padrões |
| [concepts/responsabilidades-das-camadas.md](concepts/responsabilidades-das-camadas.md) | O que entra (e o que NÃO entra) em cada camada |
| [concepts/idempotencia-e-reprocessamento.md](concepts/idempotencia-e-reprocessamento.md) | Rodar de novo sem efeitos colaterais |
| [patterns/contratos-e-nomenclatura.md](patterns/contratos-e-nomenclatura.md) | Padrões de nome e o contrato de cada fronteira |
| [patterns/modelagem-silver-e-gold.md](patterns/modelagem-silver-e-gold.md) | 1:1 vs domínio na Silver; agregações na Gold |

## Domínios relacionados

- `delta-lake/` — formato das tabelas em todas as camadas
- `databricks-lakeflow/` — implementação declarativa das camadas
- `qualidade-de-dados/` — expectativas aplicadas nas fronteiras
- `data-mesh/` — organização dos dados por domínio sobre as camadas
