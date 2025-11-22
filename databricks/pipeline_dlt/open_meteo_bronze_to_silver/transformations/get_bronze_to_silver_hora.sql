-- ===========================================================
-- SILVER: clima_horario
-- Depende de open_meteo.bronze.clima_horario
-- Conversão de tipos, de-para UF, normalização
-- ===========================================================

CREATE OR REFRESH STREAMING LIVE TABLE open_meteo.silver.clima_horario
COMMENT "Silver — Clima horário limpo, tipado, enriquecido e pronto para analytics"
TBLPROPERTIES ("quality" = "silver")
PARTITIONED BY (ano,mes)
AS
WITH bronze AS (

  SELECT
      CAST(data_hora AS TIMESTAMP) AS data_hora,
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

      ingested_at
  FROM STREAM(open_meteo.bronze.clima_horario)
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
    municipio,
    uf,
    latitude,
    longitude,

    COALESCE(temperatura_c, 0) AS temperatura_c,
    COALESCE(umidade_relativa, 0) AS umidade_relativa,
    COALESCE(precipitacao_mm, 0) AS precipitacao_mm,
    COALESCE(velocidade_vento_ms, 0) AS velocidade_vento_ms,

    ano,
    mes,
    dia,
    ingested_at

FROM stage;