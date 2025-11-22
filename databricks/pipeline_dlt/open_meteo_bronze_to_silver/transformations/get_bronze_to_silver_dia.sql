-- ===========================================================
-- SILVER: clima_diario
-- Depende do bronze: open_meteo.bronze.clima_diario
-- Aplica conversão de tipos, normalização
-- ===========================================================

CREATE OR REFRESH STREAMING LIVE TABLE open_meteo.silver.clima_diario
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
  FROM STREAM(open_meteo.bronze.clima_diario)
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

    COALESCE(temp_max_c, 0) AS temp_max_c,
    COALESCE(temp_min_c, 0) AS temp_min_c,
    COALESCE(sensacao_termica_max_c, 0) AS sensacao_termica_max_c,
    COALESCE(sensacao_termica_min_c, 0) AS sensacao_termica_min_c,
    COALESCE(precipitacao_total_mm, 0) AS precipitacao_total_mm,
    COALESCE(chuva_mm, 0) AS chuva_mm,
    COALESCE(neve_mm, 0) AS neve_mm,
    COALESCE(vento_velocidade_max_kmh, 0) AS vento_velocidade_max_kmh,
    COALESCE(rajadas_vento_max_kmh, 0) AS rajadas_vento_max_kmh,
    COALESCE(vento_direcao_dominante_graus, 0) AS vento_direcao_dominante_graus,
    COALESCE(radiacao_solar_mj_m2, 0) AS radiacao_solar_mj_m2,

    codigo_tempo_wmo,
    ano,
    mes,
    dia,
    ingested_at

FROM stage;