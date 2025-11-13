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

## 👤 Autor

guigeo

---

**Última atualização**: Novembro de 2025