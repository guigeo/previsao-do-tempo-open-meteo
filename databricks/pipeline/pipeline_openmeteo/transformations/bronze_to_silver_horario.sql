-- ===========================================================
-- SILVER: clima_horario
-- Depende de open_meteo.bronze.clima_horario
-- Conversão de tipos, de-para UF, normalização
--
-- GAP CONHECIDO: registros horários anteriores a 2026-03-06 têm
-- codigo_ibge e fonte NULL — o Parquet de origem no S3 daquele
-- período foi gerado por uma versão do extrator que não incluía
-- essas colunas. Decisão: manter como gap documentado em vez de
-- reprocessar/backfillar (custo não justificou para o uso atual).
-- ===========================================================

CREATE OR REFRESH STREAMING LIVE TABLE open_meteo.silver.clima_hora_hora (
  CONSTRAINT codigo_ibge_presente EXPECT (codigo_ibge IS NOT NULL),
  CONSTRAINT valid_coordenadas EXPECT (latitude BETWEEN -34 AND 6 AND longitude BETWEEN -75 AND -28) ON VIOLATION DROP ROW,
  CONSTRAINT valid_umidade EXPECT (umidade_relativa IS NULL OR umidade_relativa BETWEEN 0 AND 100) ON VIOLATION DROP ROW,
  CONSTRAINT valid_vento EXPECT (velocidade_vento_ms IS NULL OR velocidade_vento_ms >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_precipitacao EXPECT (precipitacao_mm IS NULL OR precipitacao_mm >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT plausivel_temperatura EXPECT (temperatura_c IS NULL OR temperatura_c BETWEEN -15 AND 50)
)
COMMENT "Silver — Clima horário limpo, tipado, enriquecido e pronto para analytics"
TBLPROPERTIES ("quality" = "silver")
PARTITIONED BY (ano,mes)
AS
WITH bronze AS (

  SELECT
      CAST(data_hora AS TIMESTAMP) AS data_hora,
      CAST(codigo_ibge AS BIGINT) AS codigo_ibge,
      UPPER(municipio) AS municipio,

      CASE uf
        WHEN 'Acre' THEN 'AC'
        WHEN 'Alagoas' THEN 'AL'
        WHEN 'Amapá' THEN 'AP'
        WHEN 'Amazonas' THEN 'AM'
        WHEN 'Bahia' THEN 'BA'
        WHEN 'Ceará' THEN 'CE'
        WHEN 'Distrito Federal' THEN 'DF'
        WHEN 'Espírito Santo' THEN 'ES'
        WHEN 'Goiás' THEN 'GO'
        WHEN 'Maranhão' THEN 'MA'
        WHEN 'Mato Grosso' THEN 'MT'
        WHEN 'Mato Grosso do Sul' THEN 'MS'
        WHEN 'Minas Gerais' THEN 'MG'
        WHEN 'Pará' THEN 'PA'
        WHEN 'Paraíba' THEN 'PB'
        WHEN 'Paraná' THEN 'PR'
        WHEN 'Pernambuco' THEN 'PE'
        WHEN 'Piauí' THEN 'PI'
        WHEN 'Rio de Janeiro' THEN 'RJ'
        WHEN 'Rio Grande do Norte' THEN 'RN'
        WHEN 'Rio Grande do Sul' THEN 'RS'
        WHEN 'Rondônia' THEN 'RO'
        WHEN 'Roraima' THEN 'RR'
        WHEN 'Santa Catarina' THEN 'SC'
        WHEN 'São Paulo' THEN 'SP'
        WHEN 'Sergipe' THEN 'SE'
        WHEN 'Tocantins' THEN 'TO'
      END AS uf,

      CAST(latitude AS DOUBLE) AS latitude,
      CAST(longitude AS DOUBLE) AS longitude,

      CAST(temperatura_c AS DOUBLE) AS temperatura_c,
      CAST(umidade_relativa AS DOUBLE) AS umidade_relativa,
      CAST(precipitacao_mm AS DOUBLE) AS precipitacao_mm,
      CAST(velocidade_vento_ms AS DOUBLE) AS velocidade_vento_ms,

      LOWER(fonte) AS fonte,

      ingested_at
  FROM STREAM(open_meteo.bronze.clima_hora_hora)
),

stage AS (
  SELECT
      *,
      YEAR(data_hora) AS ano,
      MONTH(data_hora) AS mes,
      DAY(data_hora) AS dia
  FROM bronze
)

SELECT
    data_hora,
    codigo_ibge,
    municipio,
    uf,
    latitude,
    longitude,

    temperatura_c,
    umidade_relativa,
    precipitacao_mm,
    velocidade_vento_ms,

    COALESCE(fonte, 'archive') AS fonte,

    ano,
    mes,
    dia,
    ingested_at

FROM stage;