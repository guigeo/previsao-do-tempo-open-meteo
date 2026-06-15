# Contratos e Nomenclatura

> **Propósito**: Padrões de nome e contratos explícitos em cada fronteira de camada
> **Validado**: 2026-06-10

## Quando usar

- Criando o primeiro pipeline de um lakehouse novo
- Padronizando um lakehouse que cresceu sem convenção
- Definindo o que a camada seguinte pode assumir como garantido

## Implementação

```text
Esquema Unity Catalog: {catalogo}.{camada}.{entidade}[_{qualificador}]

bronze.stations_raw          # espelho da origem; sufixo _raw opcional
bronze.stations_snapshots    # snapshots periódicos carimbados
silver.stations              # entidade limpa — nome de negócio, singular do domínio
silver.stations_quarantine   # linhas reprovadas nas expectativas
gold.stations_by_city        # visão de consumo — nome diz a pergunta que responde
gold.kpi_coverage_monthly    # prefixo kpi_ para métricas oficiais

Colunas técnicas (sempre com _ prefixado):
_ingested_at, _source_file, _updated_at, _row_hash
```

Contrato mínimo de cada fronteira (documentar na descrição da tabela):

```text
Bronze → Silver garante: nada (dado cru). Silver deve validar tudo.
Silver → Gold garante: chave única, tipos corretos, sem nulos nas colunas NOT NULL,
                       referências conformadas (UF, município válidos).
Gold → consumidor garante: regra de negócio documentada, SLA de atualização.
```

## Configuração

| Convenção | Valor | Observação |
|-----------|-------|------------|
| Idioma dos nomes | um só (decidir e manter) | misturar PT/EN é o erro mais comum |
| Caixa | `snake_case` | compatível com SQL em todo lugar |
| Prefixo técnico | `_` | separa metadado de pipeline de dado de negócio |
| Comentários de tabela | obrigatórios na Gold | `COMMENT ON TABLE` com a definição de negócio |

## Exemplo de uso

```sql
COMMENT ON TABLE gold.kpi_coverage_monthly IS
'Clientes ativos por município/mês. Ativo = assinatura vigente na data de corte.
 Fonte: silver.stations. Atualização: mensal, dia 5.';
```

## Ver também

- [modelagem-silver-e-gold](modelagem-silver-e-gold.md)
- [responsabilidades-das-camadas](../concepts/responsabilidades-das-camadas.md)
