# Previsão do Tempo - Open-Meteo 🌤️

Coleta automatizada de dados climáticos para municípios brasileiros usando a API [Open-Meteo](https://open-meteo.com/).

## 📌 Sobre

Sistema Python que coleta dados meteorológicos históricos (D-1) de forma robusta e eficiente, com suporte a:
- **Dados Diários**: Temperatura máx/mín, sensação térmica, precipitação, neve, vento, radiação solar
- **Dados Horários**: Temperatura, umidade relativa, precipitação, velocidade do vento
- **Multi-município**: Processa simultaneamente todos os municípios da lista
- **Retry automático**: Tratamento inteligente de falhas de conexão

## 🚀 Quick Start

### Pré-requisitos
- Python 3.8+
- pip

### Instalação

```bash
pip install -r requirements.txt
```

### Uso

Coletar dados de **ambos os modos** (padrão):
```bash
python main.py
```

Apenas dados **diários**:
```bash
python main.py --modo diario
```

Apenas dados **horários**:
```bash
python main.py --modo horario
```

### Saída

Os arquivos são salvos em `data/raw/` com nomes padronizados:
- `dados_climaticos_diarios_YYYYMMDD.csv` (dados diários)
- `dados_climaticos_horarios_YYYYMMDD.csv` (dados horários)

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
├── main.py                          # Script principal com CLI
├── requirements.txt                 # Dependências Python
├── README.md                        # Este arquivo
├── src/
│   ├── recupera_dados_api_dia.py   # Coleta dados diários
│   ├── recupera_dados_api_hora.py  # Coleta dados horários
│   └── processa_dados.py           # Processamento e tradução
└── data/
    ├── lista_municipios/
    │   └── lista_mun.csv           # Municípios com coordenadas
    └── raw/                         # CSVs coletados
```

## 🛠️ Dependências

- **pandas**: Processamento de dados
- **requests**: Requisições HTTP à API
- **python-dateutil**: Manipulação de datas/timezones
- **tqdm**: Barra de progresso
- **pytz**: Suporte a timezones

## ⚙️ Configuração

As configurações principais estão em `main.py`:

```python
MODO_COLETA_DEFAULT = "ambos"  # "diario" | "horario" | "ambos"
TIMEZONE = "America/Sao_Paulo"
```

## 🔄 Recursos

- ✅ Coleta D-1 (dados do dia anterior)
- ✅ Retry automático com backoff exponencial
- ✅ Suporte a múltiplos timezones
- ✅ Tratamento robusto de erros
- ✅ Progresso visual (tqdm)
- ✅ Encoding UTF-8 com BOM para Excel

## 📝 Notas

- A API Open-Meteo é **gratuita** e não requer autenticação
- Coleta sempre o D-1 (dia anterior) considerando timezone de São Paulo
- Em caso de falha na API, há retry automático (máx. 2 tentativas)
- Dados horários possuem fallback entre archive e forecast APIs

## 📖 API Utilizada

[Open-Meteo](https://open-meteo.com/) - API climática gratuita e open-source com dados históricos e previsões.

**Última atualização**: Novembro de 2025
## Backfill histórico

O repositório inclui um script de backfill para popular o conjunto de dados históricos em Parquet e enviar para um bucket S3.

- Script: `scripts/backfill_once.py`
- Gera arquivos Parquet por dia em `data/raw/diario/` e `data/raw/horario/`.
- Configurações principais (no topo do script): `DATA_INI`, `DATA_FIM`, `BUCKET`, `PROFILE`, `PARQUET_COMPRESSION`.
- Compressão Parquet: `snappy` (padrão). Engine: `pyarrow`.

Exemplo de execução:
```bash
python scripts/backfill_once.py
```

O script itera sobre a lista de municípios (`data/lista_municipios/lista_mun.csv`), faz chamadas ao endpoint `archive` da Open-Meteo para cada dia e município, salva os Parquets localmente e realiza upload para S3.

## Upload para S3

Existe um utilitário em `src/upload_s3.py` para enviar arquivos ao S3 usando `boto3` e um profile AWS configurado.

- Função: `upload_para_s3(caminho_local, tipo, data_referencia, bucket, profile)`
- Prefixo S3 (padrão Hive-style): `raw/clima/{tipo}/date=YYYY-MM-DD/{nome_arquivo}`
- Bucket padrão usado no projeto: `gbrj-open-meteo-datalake` (pode ser alterado no call)

Requisitos para upload:
- Ter o `boto3` instalado (geralmente já disponível em ambientes que usam AWS SDKs)
- Ter um profile AWS configurado no `~/.aws/credentials` com o nome passado no parâmetro `profile` (ex: `open-meteo`)

Exemplo de uso (via script de backfill):
```py
from src.upload_s3 import upload_para_s3

upload_para_s3(caminho_local='data/raw/diario/dados_climaticos_diarios_20251106.parquet',
               tipo='diario',
               data_referencia='2025-11-06',
               bucket='gbrj-open-meteo-datalake',
               profile='open-meteo')
```

## Atualizações na Estrutura do Projeto

Adições relevantes:

```
previsao-do-tempo-open-meteo/
├── main.py                          # Script principal com CLI
├── requirements.txt                 # Dependências Python
├── README.md                        # Este arquivo
├── scripts/
│   └── backfill_once.py            # Backfill histórico + upload S3
├── src/
│   ├── recupera_dados_api_dia.py   # Coleta dados diários
│   ├── recupera_dados_api_hora.py  # Coleta dados horários
│   ├── processa_dados.py           # Processamento e tradução
│   └── upload_s3.py                # Utilitário de upload para S3 (boto3)
└── data/
    ├── lista_municipios/
    │   └── lista_mun.csv           # Municípios com coordenadas
    └── raw/                         # CSVs/Parquets coletados
```

## Observações importantes

- O script de backfill pode gerar uma carga considerável de requisições à Open-Meteo — ajuste `SLEEP_BETWEEN_CALLS` e `RETRIES` conforme necessário.
- Confira permissões e custo de armazenamento/transferência do bucket S3 antes de fazer uploads em massa.
- Teste localmente com um subconjunto pequeno de municípios antes de rodar backfills grandes.

## 👤 Autor

guigeo

---

**Última atualização**: Novembro de 2025