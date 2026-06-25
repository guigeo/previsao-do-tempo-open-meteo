-- ===========================================================
-- SILVER: clima_diario
-- Depende do bronze: open_meteo.bronze.clima_diario
-- Aplica conversão de tipos, normalização
-- ===========================================================

CREATE OR REFRESH STREAMING LIVE TABLE open_meteo.silver.clima_dia_dia (
  CONSTRAINT valid_codigo_ibge EXPECT (codigo_ibge IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_coordenadas EXPECT (latitude BETWEEN -34 AND 6 AND longitude BETWEEN -75 AND -28) ON VIOLATION DROP ROW,
  CONSTRAINT valid_precipitacao EXPECT (precipitacao_total_mm IS NULL OR precipitacao_total_mm >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_chuva EXPECT (chuva_mm IS NULL OR chuva_mm >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_neve EXPECT (neve_mm IS NULL OR neve_mm >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT plausivel_temp_max EXPECT (temp_max_c IS NULL OR temp_max_c BETWEEN -15 AND 50),
  CONSTRAINT plausivel_temp_min EXPECT (temp_min_c IS NULL OR temp_min_c BETWEEN -15 AND 50),
  CONSTRAINT plausivel_vento_direcao EXPECT (vento_direcao_dominante_graus IS NULL OR vento_direcao_dominante_graus BETWEEN 0 AND 360)
)
COMMENT "Silver — Clima diário limpo, tipado, deduplicado e pronto para analytics"
TBLPROPERTIES ("quality" = "silver")
PARTITIONED BY (ano,mes)
AS
WITH bronze AS (

  SELECT
      CAST(data AS DATE) AS data,
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

      CAST(temp_max_c AS DOUBLE) AS temp_max_c,
      CAST(temp_min_c AS DOUBLE) AS temp_min_c,
      CAST(sensacao_termica_max_c AS DOUBLE) AS sensacao_termica_max_c,
      CAST(sensacao_termica_min_c AS DOUBLE) AS sensacao_termica_min_c,
      CAST(precipitacao_total_mm AS DOUBLE) AS precipitacao_total_mm,
      CAST(chuva_mm AS DOUBLE) AS chuva_mm,
      CAST(neve_mm AS DOUBLE) AS neve_mm,
      CAST(vento_velocidade_max_kmh AS DOUBLE) AS vento_velocidade_max_kmh,
      CAST(rajadas_vento_max_kmh AS DOUBLE) AS rajadas_vento_max_kmh,
      CAST(vento_direcao_dominante_graus AS DOUBLE) AS vento_direcao_dominante_graus,
      CAST(radiacao_solar_mj_m2 AS DOUBLE) AS radiacao_solar_mj_m2,

      CAST(codigo_tempo_wmo AS INT) AS codigo_tempo_wmo,
      ingested_at
  FROM STREAM(open_meteo.bronze.clima_dia_dia)
),

stage AS (
  SELECT
      *,
      YEAR(data) AS ano,
      MONTH(data) AS mes,
      DAY(data) AS dia
  FROM bronze
)

SELECT
    data,
    codigo_ibge,
    municipio,
    uf,
    latitude,
    longitude,

    temp_max_c,
    temp_min_c,
    sensacao_termica_max_c,
    sensacao_termica_min_c,
    precipitacao_total_mm,
    chuva_mm,
    neve_mm,
    vento_velocidade_max_kmh,
    rajadas_vento_max_kmh,
    vento_direcao_dominante_graus,
    radiacao_solar_mj_m2,

    codigo_tempo_wmo,
    ano,
    mes,
    dia,
    ingested_at

FROM stage;