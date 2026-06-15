# Arquitetura Medalhão — Quick Reference

> Tabelas de consulta rápida. Exemplos nos arquivos linkados.

## Resumo por camada

| Camada | Conteúdo | Transformação permitida | Consumidor |
|--------|----------|------------------------|------------|
| Bronze | Dado cru como chegou + metadados de ingestão | Nenhuma (só anexar `_ingested_at`, `_source_file`) | Silver |
| Silver | Dado limpo, tipado, deduplicado, conformado | Limpeza, tipos, dedup, joins de conformação | Gold, análises ad-hoc |
| Gold | Agregações e modelos prontos para consumo | Regras de negócio, KPIs, denormalização | BI, dashboards, APIs |

## Matriz de decisão

| Caso | Camada |
|------|--------|
| Converter string → date, normalizar encoding | Silver |
| Deduplicar por chave natural | Silver |
| Regra de negócio ("cliente ativo = assinatura vigente") | Gold (ou Silver se for definição de entidade) |
| Junção com tabela de referência (UF, município) | Silver (conformação) |
| KPI agregado por período/região | Gold |
| Corrigir dado errado da origem | NUNCA editar Bronze — corrigir na Silver com regra explícita |

## Anti-padrões

| Não faça | Faça |
|----------|------|
| Limpar/filtrar dados na Bronze | Bronze é cru e imutável — auditoria e replay dependem disso |
| Gold lendo direto da Bronze | Gold consome só Silver — a limpeza não pode ser pulada |
| Lógica de negócio espalhada nas 3 camadas | Concentrar regra de negócio na Gold (documentada) |
| Tabela "temp_final_v2_nova" | Nomenclatura padronizada por camada (ver patterns) |
| Reprocessar com append cego | Cargas idempotentes: MERGE ou overwrite particionado |

## Documentação relacionada

| Tópico | Caminho |
|--------|---------|
| Responsabilidades | `concepts/responsabilidades-das-camadas.md` |
| Idempotência | `concepts/idempotencia-e-reprocessamento.md` |
| Índice completo | `index.md` |
