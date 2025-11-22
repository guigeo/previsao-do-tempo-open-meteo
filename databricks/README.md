# Pipeline Databricks — Medalion Architecture 🏗️

## 📌 Visão Geral

Este diretório contém os pipelines **Delta Live Tables (DLT)** que processam dados climáticos seguindo a arquitetura **Medalion** (Bronze → Silver → Gold). Os dados são ingeridos do **S3** (enviados pela aplicação Python/Docker) e transformados em tabelas otimizadas no **Databricks Lakehouse**.

### Fluxo de Dados

```
┌─────────────────────┐
│  S3 Raw (Parquet)   │  ← Python/Docker escreve aqui
│ /raw/clima/         │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────────────────┐
│  BRONZE LAYER (open_meteo_s3_to_bronze)        │
│  - Leitura via cloud_files()                    │
│  - Schema simples (tudo strings)                │
│  - Streaming Live Tables (SLT)                  │
│  - Tabelas: clima_diario, clima_horario         │
└──────────┬──────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────┐
│  SILVER LAYER (open_meteo_bronze_to_silver)    │
│  - Transformação de tipos                       │
│  - Lógica de negócio                            │
│  - Deduplica, normaliza, enriquece              │
│  - Tabelas: clima_diario, clima_horario         │
└──────────┬──────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────┐
│  GOLD LAYER (open_meteo_silver_to_gold)        │
│  - Agregações, métricas                         │
│  - Otimizado para analytics/BI                  │
│  - Tabelas: metricas_clima                      │
└─────────────────────────────────────────────────┘
```

## 🗂️ Estrutura de Diretórios

```
databricks/
├── README.md                              # Este arquivo
├── pipeline_dlt/
│   ├── open_meteo_s3_to_bronze/
│   │   └── transformations/
│   │       ├── get_s3_to_bronze_dia.sql  # Bronze — dados diários
│   │       └── get_s3_to_bronze_hora.sql # Bronze — dados horários
│   │
│   ├── open_meteo_bronze_to_silver/
│   │   └── transformations/
│   │       ├── get_bronze_to_silver_dia.sql
│   │       └── get_bronze_to_silver_hora.sql
│   │
│   └── open_meteo_silver_to_gold/
│       └── transformations/
│           └── gold_metricas_clima.sql
│
└── config/
    └── dlt_config.json (não versioned — configuração local)
```

## 🚀 Setup no Databricks

### Pré-requisitos

1. **Workspace Databricks** ativo (Premium ou acima para DLT)
2. **Cluster** com DBR 13.3+ (runtime com suporte a DLT)
3. **Permissões** para criar catálogo e schemas
4. **Acesso ao S3** via IAM role ou credenciais

### Passos de Configuração

#### 1. Crie um Catálogo e Schema

```sql
-- No Databricks SQL Editor:
CREATE CATALOG IF NOT EXISTS open_meteo;
CREATE SCHEMA IF NOT EXISTS open_meteo.bronze;
CREATE SCHEMA IF NOT EXISTS open_meteo.silver;
CREATE SCHEMA IF NOT EXISTS open_meteo.gold;

-- Defina permissões se necessário
GRANT ALL PRIVILEGES ON CATALOG open_meteo TO `seu-grupo@empresa.com`;
```

#### 2. Configure Acesso ao S3

Se usando IAM role:
```python
# No Databricks Python REPL:
# Sua workspace já tem uma IAM role anexada
# Verifique na console AWS que o bucket S3 está acessível
```

Alternativa com credenciais:
```python
# Configure em Databricks Admin > Secrets
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get("scope=aws", "access_key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get("scope=aws", "secret_key"))
```

#### 3. Crie um DLT Pipeline

No Databricks Workspace:
- Vá para **Workflows** → **Delta Live Tables** → **Create Pipeline**
- Preecha as informações:
  - **Pipeline name:** `open-meteo-medalion`
  - **Notebook or SQL path:** `/Users/seu-email@empresa.com/open_meteo_s3_to_bronze` (ou similar, depende de onde você salvou)
  - **Target catalog:** `open_meteo`
  - **Target schema:** `bronze`
  - **Cluster:** Selecione um cluster DLT ou auto-scaling
  - **Modo:** `Development` (para testes) ou `Continuous` (para produção)

#### 4. Configure Variáveis de Pipeline

Adicione as seguintes variáveis na aba **Advanced** do pipeline:

```json
{
  "input_path_diario": "s3://seu-bucket/raw/clima/diario/",
  "input_path_horario": "s3://seu-bucket/raw/clima/horario/",
  "environment": "dev"
}
```

#### 5. Execute o Pipeline

Clique em **Start** para rodar o pipeline. Os dados do S3 serão lidos e as tabelas serão criadas/atualizadas.

## 📊 Estrutura de Dados por Camada

### Bronze (Raw)

**Tabela:** `open_meteo.bronze.clima_diario`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| data | STRING | Data do registro (YYYY-MM-DD) |
| codigo_ibge | STRING | Código IBGE do município |
| municipio | STRING | Nome do município |
| uf | STRING | Unidade federativa |
| latitude | STRING | Latitude (coordenada) |
| longitude | STRING | Longitude (coordenada) |
| temp_max_c | STRING | Temperatura máxima (°C) |
| temp_min_c | STRING | Temperatura mínima (°C) |
| ... | STRING | Outros campos meteorológicos |
| ingested_at | TIMESTAMP | Data/hora de ingestão |

### Silver (Transformado)

Tipos corrigidos (DOUBLE para temp, INT para código_ibge, etc.), lógica de negócio aplicada, deduplicação.

### Gold (Analytics)

Tabelas agregadas e métricas prontas para BI/Analytics.

## 🛠️ Modificar Transformações

Para ajustar lógica das transformações:

1. Edite os arquivos `.sql` nos respectivos diretórios
2. O DLT atualiza automaticamente em cada execução (modo `Continuous`) ou clique em **Start**
3. Para testes, execute SQL direto no Databricks Editor:

```sql
-- Teste uma transformação individualmente
CREATE OR REPLACE TABLE open_meteo.bronze.clima_diario_test AS
SELECT * FROM cloud_files(
  "s3://seu-bucket/raw/clima/diario/",
  "parquet"
);

SELECT * FROM open_meteo.bronze.clima_diario_test LIMIT 10;
```

## 🔗 Integração com Python

O Python Docker envia Parquets para S3. O pipeline DLT detecta automaticamente novos arquivos e processa.

**Prefixo S3 esperado:**
```
s3://gbrj-open-meteo-datalake/raw/clima/diario/date=2025-11-21/dados_climaticos_diarios_20251121.parquet
s3://gbrj-open-meteo-datalake/raw/clima/horario/date=2025-11-21/dados_climaticos_horarios_20251121.parquet
```

## 📋 Checklista de Deployment

- [ ] Catalog e schemas criados
- [ ] Acesso ao S3 testado
- [ ] Pipeline DLT criado
- [ ] Variáveis de pipeline configuradas
- [ ] Pipeline executado com sucesso
- [ ] Dados presentes em `open_meteo.bronze.*`
- [ ] Transformações Silver/Gold executadas
- [ ] Permissões de acesso validadas

## 🐛 Troubleshooting

### "Path does not exist" no cloud_files()

**Causa:** S3 path incorreto ou sem dados

**Solução:**
- Confirme o bucket e prefix em `input_path_diario` / `input_path_horario`
- Verifique no console AWS se há objetos Parquet nesse path
- Confirme credenciais de acesso

### "Insufficient permissions"

**Causa:** IAM role sem permissões S3

**Solução:**
- Adicione policy S3 à role Databricks:
  ```json
  {
    "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetObjectVersion"],
    "Effect": "Allow",
    "Resource": ["arn:aws:s3:::seu-bucket", "arn:aws:s3:::seu-bucket/*"]
  }
  ```

### Pipeline não atualiza

**Causa:** Modo `Development` ou cluster não rodando

**Solução:**
- Certifique-se que o cluster está ativo
- Para produção, altere para modo `Continuous`
- Clique em **Start** novamente

## 📚 Referências

- [Databricks Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/)
- [Medalion Architecture](https://www.databricks.com/blog/2022/06/24/etl-patterns-at-scale-with-medallion-architecture-and-databricks.html)
- [Cloud Files (Auto Loader)](https://docs.databricks.com/en/ingestion/cloud-object-storage/index.html)

## 👤 Suporte

Para dúvidas sobre os pipelines Databricks, consulte o time de Data Engineering.
