# Previsão do Tempo - Open-Meteo 🌤️

**Arquitetura End-to-End:** Python/Docker → S3 → Databricks Lakehouse (Medalion)

Sistema completo para coleta automatizada de dados climáticos, processamento em pipeline e transformação em **Lakehouse** usando Databricks.

## 📌 Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                       INGESTÃO (Python/Docker)                      │
│  - Open-Meteo API (dados D-1)                                       │
│  - Coleta diária/horária para todos os municípios BR                │
│  - Salva CSV localmente ou Parquet em S3                            │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   AWS S3     │
                    │ /raw/clima/  │ ← Dados brutos (Parquet)
                    └──────────────┘
                           │
                           ▼
         ┌─────────────────────────────────────────┐
         │   DATABRICKS LAKEHOUSE (Medalion)       │
         │  ┌────────────────────────────────────┐ │
         │  │  Bronze: Raw data                  │ │
         │  │  (cloud_files + SLT)               │ │
         │  └────────────┬───────────────────────┘ │
         │               │                         │
         │  ┌────────────▼───────────────────────┐ │
         │  │  Silver: Transformed & Clean       │ │
         │  │  (dedupe, tipos, normalização)     │ │
         │  └────────────┬───────────────────────┘ │
         │               │                         │
         │  ┌────────────▼───────────────────────┐ │
         │  │  Gold: Metrics & Analytics         │ │
         │  │  (agregações, KPIs)                │ │
         │  └────────────────────────────────────┘ │
         └─────────────────────────────────────────┘
```

## 📋 Sobre o Projeto

Sistema Python que coleta dados meteorológicos históricos (D-1) de forma robusta e eficiente, com suporte a:
- **Dados Diários**: Temperatura máx/mín, sensação térmica, precipitação, neve, vento, radiação solar
- **Dados Horários**: Temperatura, umidade relativa, precipitação, velocidade do vento
- **Multi-município**: Processa simultaneamente todos os municípios brasileiros
- **Retry automático**: Tratamento inteligente de falhas de conexão
- **Upload S3**: Salva dados em Parquet com particionamento Hive-style
- **Databricks DLT**: Pipelines de transformação automatizados (Bronze → Silver → Gold)

## 🚀 Quick Start

### ⚠️ IMPORTANTE: Configuração de Credenciais

**NÃO commite o arquivo `.env` com credenciais reais!**

Este projeto usa `.env` para armazenar credenciais AWS. Para segurança:

1. Copie `.env.example` para `.env` (local, não versionado)
2. Preencha com suas credenciais reais
3. `.gitignore` garante que `.env` nunca será commitado

```bash
cp .env.example .env
# Edite .env com suas credenciais reais
```

### Pré-requisitos

- **Python 3.8+**
- **pip** ou **conda**
- **Docker** (para rodar containerizado)
- **AWS Credentials** (para upload S3)
- **Databricks Account** (para pipeline de transformação)

### Instalação Local

#### 1. Clone o repositório

```bash
git clone https://github.com/guigeo/previsao-do-tempo-open-meteo.git
cd previsao-do-tempo-open-meteo
```

#### 2. Crie e ative um ambiente virtual

macOS / Linux (zsh/bash):
```bash
python -m venv .venv
source .venv/bin/activate
```

Windows (PowerShell):
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

#### 3. Instale dependências

```bash
pip install -r requirements.txt
```

#### 4. Configure credenciais AWS (opcional para upload S3)

```bash
# Configure um profile AWS (ex: "open-meteo")
aws configure --profile open-meteo
# Forneça Access Key, Secret Key, region, output
```

Ou edite `.env` diretamente:
```bash
AWS_ACCESS_KEY_ID=sua-chave
AWS_SECRET_ACCESS_KEY=sua-secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=seu-bucket-s3
```

#### 5. Verifique a lista de municípios

```bash
# Arquivo deve existir e conter: codigo_ibge, nome, nome_uf, latitude, longitude
head data/lista_municipios/lista_mun.csv
```

## Passo a passo — executar o projeto

### Execução Local (Python)

#### 1. Coleta diária/horária

```bash
# Coleta modo padrão (ambos: diário + horário)
python main.py

# Apenas diários
python main.py --modo diario

# Apenas horários
python main.py --modo horario
```

Os arquivos gerados ficam em `data/raw/` com nomes padronizados:
- `dados_climaticos_diarios_YYYYMMDD.csv`
- `dados_climaticos_horarios_YYYYMMDD.csv`

#### 2. Backfill histórico + upload S3

```bash
python scripts/backfill_once.py
```

Este script:
- Faz fetch histórico (endpoint `archive` da Open-Meteo)
- Salva em Parquet (compressão snappy) em `data/raw/diario/` e `data/raw/horario/`
- Realiza upload para S3 automaticamente

**Edite o topo do script para alterar:**
- `DATA_INI` / `DATA_FIM` (intervalo de datas)
- `SLEEP_BETWEEN_CALLS` (para throttling)
- `BUCKET` / `PROFILE` (destino S3)

### Execução via Docker

#### 1. Build da imagem

```bash
docker-compose build --no-cache
```

#### 2. Executar container

```bash
# Executa com comando padrão (python main.py --modo ambos)
docker-compose up --abort-on-container-exit

# Rodar em background
docker-compose up -d
docker-compose logs -f
```

#### 3. Customizar comando

Edite `docker-compose.yml` e altere o `command`:
```yaml
services:
  openmeteo:
    # ...
    command: ["python", "main.py", "--modo", "diario"]
```

Ou sobrescreva na CLI:
```bash
docker-compose run --rm openmeteo python main.py --modo horario
```

#### 4. Variáveis de ambiente

O `docker-compose.yml` lê do arquivo `.env`. Certifique-se que contém:
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=seu-bucket
```

### Pipeline Databricks (Transformação)

Para configurar e executar o pipeline de transformação no Databricks, consulte **[databricks/README.md](./databricks/README.md)**.

Resumo:
1. Crie um catálogo `open_meteo` com schemas `bronze`, `silver`, `gold`
2. Configure acesso ao S3 via IAM role ou secrets
3. Crie um pipeline DLT apontando para os scripts SQL em `databricks/pipeline_dlt/`
4. Execute: os dados do S3 serão ingeridos e transformados automaticamente

## 📊 Estrutura de Dados

### Dados Diários
| Campo | Descrição |
|-------|-----------|
| `data` | Data (formato YYYY-MM-DD) |
| `temp_max_c` | Temperatura máxima (°C) |
| `temp_min_c` | Temperatura mínima (°C) |
| `sensacao_termica_max_c` | Sensação térmica máxima (°C) |
| `sensacao_termica_min_c` | Sensação térmica mínima (°C) |
| `precipitacao_total_mm` | Precipitação total (mm) |
| `chuva_mm` | Chuva (mm) |
| `neve_mm` | Neve (mm) |
| `vento_velocidade_max_kmh` | Velocidade máx do vento (km/h) |
| `rajadas_vento_max_kmh` | Rajadas máx do vento (km/h) |
| `vento_direcao_dominante_graus` | Direção dominante do vento (°) |
| `radiacao_solar_mj_m2` | Radiação solar (MJ/m²) |
| `codigo_tempo_wmo` | Código WMO do tempo |
| `municipio`, `uf`, `latitude`, `longitude` | Dados do município |

### Dados Horários
| Campo | Descrição |
|-------|-----------|
| `data_hora` | Data/hora (ISO 8601) |
| `temperatura_c` | Temperatura (°C) |
| `umidade_relativa` | Umidade relativa (%) |
| `precipitacao_mm` | Precipitação (mm) |
| `velocidade_vento_ms` | Velocidade do vento (m/s) |
| `municipio`, `uf`, `latitude`, `longitude` | Dados do município |

## 🗂️ Estrutura do Projeto

```
previsao-do-tempo-open-meteo/
├── .env.example                     # Template de credenciais (versionado)
├── .gitignore                       # Exclui .env, credenciais, dados
├── Dockerfile                       # Imagem Docker
├── docker-compose.yml               # Orquestração Docker
├── README.md                        # Este arquivo
├── requirements.txt                 # Dependências Python
├── main.py                          # Script principal com CLI
│
├── src/
│   ├── recupera_dados_api_dia.py   # Coleta dados diários
│   ├── recupera_dados_api_hora.py  # Coleta dados horários
│   ├── processa_dados.py           # Processamento e tradução
│   └── upload_s3.py                # Utilitário de upload S3 (boto3)
│
├── scripts/
│   └── backfill_once.py            # Backfill histórico + upload S3
│
├── databricks/
│   ├── README.md                   # Guia de pipeline Databricks
│   └── pipeline_dlt/
│       ├── open_meteo_s3_to_bronze/
│       │   └── transformations/
│       │       ├── get_s3_to_bronze_dia.sql
│       │       └── get_s3_to_bronze_hora.sql
│       ├── open_meteo_bronze_to_silver/
│       │   └── transformations/
│       │       ├── get_bronze_to_silver_dia.sql
│       │       └── get_bronze_to_silver_hora.sql
│       └── open_meteo_silver_to_gold/
│           └── transformations/
│               └── gold_metricas_clima.sql
│
└── data/
    ├── lista_municipios/
    │   └── lista_mun.csv           # Municípios com coordenadas
    └── raw/
        ├── diario/                 # Dados diários (local)
        └── horario/                # Dados horários (local)
```

## 🛠️ Dependências

- **pandas**: Processamento de dados
- **requests**: Requisições HTTP à API
- **python-dateutil**: Manipulação de datas/timezones
- **tqdm**: Barra de progresso
- **pytz**: Suporte a timezones
- **boto3**: Cliente AWS S3

## ⚙️ Configuração

As configurações principais estão em `main.py`:

```python
MODO_COLETA_DEFAULT = "ambos"  # "diario" | "horario" | "ambos"
TIMEZONE = "America/Sao_Paulo"
```

Para backfill, edite o topo de `scripts/backfill_once.py`:

```python
DATA_INI = date(2025, 11, 6)
DATA_FIM = date(2025, 11, 11)
BUCKET = BUCKET
```

## 🔄 Recursos

- ✅ Coleta D-1 (dados do dia anterior)
- ✅ Retry automático com backoff exponencial
- ✅ Suporte a múltiplos timezones
- ✅ Tratamento robusto de erros
- ✅ Progresso visual (tqdm)
- ✅ Encoding UTF-8 com BOM para Excel
- ✅ Upload S3 com particionamento Hive-style
- ✅ Databricks DLT (Bronze → Silver → Gold)
- ✅ Docker + Docker Compose

## 📝 Notas

- A API Open-Meteo é **gratuita** e não requer autenticação
- Coleta sempre o D-1 (dia anterior) considerando timezone de São Paulo
- Em caso de falha na API, há retry automático (máx. 2 tentativas)
- Dados horários possuem fallback entre archive e forecast APIs
- **NUNCA commite o arquivo `.env` com credenciais reais**
- Use `.env.example` como referência para novos contribuidores

## 🔒 Segurança

### Credenciais

- `.env` está no `.gitignore` e nunca será versionado
- Use `.env.example` como template — **sempre mantenha atualizado com novas variáveis**
- Para CI/CD, configure secrets no GitHub Actions ou similar

### IAM Permissions (AWS)

Recomenda-se criar um usuário IAM com permissões mínimas:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::seu-bucket",
        "arn:aws:s3:::seu-bucket/*"
      ]
    }
  ]
}
```

## 🐛 Troubleshooting

### "Arquivo não encontrado: data/lista_municipios/lista_mun.csv"

- Certifique-se que o arquivo existe
- Verifique o caminho e encoding (UTF-8 com BOM recomendado)

### "Erro de conexão com S3"

- Confirme credenciais em `.env`
- Teste: `aws s3 ls s3://seu-bucket --profile open-meteo`
- Verifique permissões IAM

### Docker não encontra `.env`

- Certifique-se que `.env` está na raiz do projeto
- Run: `docker-compose config` para validar

## 📚 Referências

- [Open-Meteo API](https://open-meteo.com/)
- [Databricks Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/)
- [Medalion Architecture](https://www.databricks.com/blog/2022/06/24/etl-patterns-at-scale-with-medallion-architecture-and-databricks.html)
- [AWS S3 + IAM](https://docs.aws.amazon.com/s3/)

## 📄 Licença

MIT

## 👤 Autor

guigeo

---

**Última atualização**: Novembro de 2025
