# Qualidade de Dados — Quick Reference

> Tabelas de consulta rápida. Exemplos nos arquivos linkados.

## Dimensões de qualidade

| Dimensão | Pergunta | Métrica típica |
|----------|----------|----------------|
| Completude | Campos obrigatórios preenchidos? | % nulos por coluna |
| Unicidade | Chave de negócio sem duplicatas? | count vs count distinct |
| Validade | Valores no formato/faixa esperados? | % linhas reprovadas por regra |
| Consistência | Bate com outras tabelas/referências? | % FKs órfãs |
| Atualidade | Dado chegou no prazo? | atraso vs SLA da fonte |
| Acurácia | Reflete o mundo real? | amostragem manual / fonte de verdade |

## Matriz de severidade → ação

| Severidade | Exemplo | Ação |
|------------|---------|------|
| Crítica (linha inutilizável) | chave primária nula | Rejeitar (drop) + contar |
| Grave (suspeita, recuperável) | coordenada fora do território | Quarentenar para análise |
| Aviso (degrada, não invalida) | campo opcional vazio | Deixar passar + métrica |
| Estrutural (lote inteiro ruim) | schema mudou, arquivo truncado | Falhar o pipeline + alertar |

## Anti-padrões

| Não faça | Faça |
|----------|------|
| `dropna()` silencioso | Toda remoção contada e logada — perda silenciosa é o pior bug |
| Validar só na entrada | Validar em cada fronteira de camada (o pipeline também cria erro) |
| Quarentena que ninguém olha | Métrica de volume de quarentena com limite que alerta |
| Regra de validade hardcoded espalhada | Expectations declarativas num só lugar por tabela |
| Dedup sem definir chave de negócio | Chave natural explícita e documentada por entidade |

## Documentação relacionada

| Tópico | Caminho |
|--------|---------|
| Dimensões | `concepts/dimensoes-de-qualidade.md` |
| Política de tratamento | `concepts/quarentena-rejeicao-alerta.md` |
| Índice completo | `index.md` |
