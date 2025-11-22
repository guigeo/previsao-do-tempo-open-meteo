-- ===========================================================
-- GOLD MAIN – Consolidação de todas as tabelas Gold
-- Projeto: previsao-do-tempo-open-meteo-databricks
-- Catalog/Scheme: open_meteo.gold
-- ===========================================================

-- ===========================================================
-- GOLD 1: clima_diario_historico
-- Uma linha por (data × cidade), com métricas derivadas
-- ===========================================================

CREATE OR REFRESH STREAMING LIVE TABLE open_meteo.gold.clima_diario_historico
COMMENT "Gold — Série histórica diária por cidade, com métricas derivadas"
TBLPROPERTIES ("quality" = "gold")
PARTITIONED BY (ano, mes)
AS
SELECT
    -- Identificação
    data,
    codigo_ibge,
    municipio,
    uf,
    latitude,
    longitude,

    -- Métricas originais
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

    -- Métricas derivadas
    (temp_max_c - temp_min_c) AS amplitude_termica_c,
    (sensacao_termica_max_c + sensacao_termica_min_c) / 2 AS sensacao_termica_media_c,

    CASE 
        WHEN chuva_mm > 0 THEN 1 
        ELSE 0 
    END AS teve_chuva,

    CASE 
        WHEN precipitacao_total_mm <= 5 THEN 'baixa'
        WHEN precipitacao_total_mm <= 20 THEN 'moderada'
        ELSE 'alta'
    END AS classificacao_chuva,

    CASE
        WHEN temp_max_c < 18 THEN 'frio'
        WHEN temp_max_c BETWEEN 18 AND 30 THEN 'agradavel'
        WHEN temp_max_c BETWEEN 30 AND 35 THEN 'quente'
        ELSE 'muito_quente'
    END AS classificacao_temperatura,

    -- Conversão MJ/m² → kWh/m²
    radiacao_solar_mj_m2 * 0.2778 AS radiacao_solar_kwh_m2,

    -- Datas auxiliares
    ano,
    mes,
    dia,

    ingested_at

FROM STREAM(open_meteo.silver.clima_diario);

----------------------------------------------------------------
-- GOLD 2: clima_extremos
-- Para cada dia, qual município registrou o MAIOR/MENOR valor
----------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE open_meteo.gold.clima_extremos
COMMENT "Gold — Extremos climáticos por dia (versão batch)"
TBLPROPERTIES ("quality" = "gold")
PARTITIONED BY (ano, mes)
AS
WITH base AS (
    SELECT
        data,
        codigo_ibge,
        municipio,
        uf,
        latitude,
        longitude,
        temp_max_c,
        temp_min_c,
        precipitacao_total_mm,
        vento_velocidade_max_kmh,
        rajadas_vento_max_kmh,
        radiacao_solar_mj_m2,
        ano,
        mes,
        dia,
        ingested_at
    FROM open_meteo.silver.clima_diario   -- 🔥 sem STREAM aqui
),

-- EXTREMOS POR DIA
max_temp AS (SELECT data, MAX(temp_max_c) AS valor FROM base GROUP BY data),
min_temp AS (SELECT data, MIN(temp_min_c) AS valor FROM base GROUP BY data),
max_chuva AS (SELECT data, MAX(precipitacao_total_mm) AS valor FROM base GROUP BY data),
max_vento AS (SELECT data, MAX(vento_velocidade_max_kmh) AS valor FROM base GROUP BY data),
max_rajada AS (SELECT data, MAX(rajadas_vento_max_kmh) AS valor FROM base GROUP BY data),
max_radiacao AS (SELECT data, MAX(radiacao_solar_mj_m2) AS valor FROM base GROUP BY data),

-- RECONTRO COM CIDADES
extremos AS (

    SELECT b.*, 'temp_max_maior' AS tipo_extremo, m.valor
    FROM base b JOIN max_temp m USING (data)
    WHERE b.temp_max_c = m.valor

    UNION ALL
    SELECT b.*, 'temp_min_menor', m.valor
    FROM base b JOIN min_temp m USING (data)
    WHERE b.temp_min_c = m.valor

    UNION ALL
    SELECT b.*, 'chuva_maior', m.valor
    FROM base b JOIN max_chuva m USING (data)
    WHERE b.precipitacao_total_mm = m.valor

    UNION ALL
    SELECT b.*, 'vento_maior', m.valor
    FROM base b JOIN max_vento m USING (data)
    WHERE b.vento_velocidade_max_kmh = m.valor

    UNION ALL
    SELECT b.*, 'rajada_maior', m.valor
    FROM base b JOIN max_rajada m USING (data)
    WHERE b.rajadas_vento_max_kmh = m.valor

    UNION ALL
    SELECT b.*, 'radiacao_maior', m.valor
    FROM base b JOIN max_radiacao m USING (data)
    WHERE b.radiacao_solar_mj_m2 = m.valor
)

SELECT * FROM extremos;


-- ===========================================================
-- GOLD 3 — Tendências Climáticas (7 dias, 30 dias, anomalias)
-- ===========================================================

CREATE OR REFRESH LIVE TABLE open_meteo.gold.clima_tendencias
COMMENT "Gold — Tendências de clima por dia e cidade (médias móveis, anomalias)"
TBLPROPERTIES ("quality" = "gold")
PARTITIONED BY (ano, mes)
AS
WITH base AS (
    SELECT
        data,
        codigo_ibge,
        municipio,
        uf,
        latitude,
        longitude,
        temp_max_c,
        temp_min_c,
        precipitacao_total_mm,
        chuva_mm,
        radiacao_solar_mj_m2,
        ano,
        mes,
        dia,
        ingested_at
    FROM open_meteo.gold.clima_diario_historico
),

enriquecida AS (
    SELECT
        *,
        (temp_max_c + temp_min_c) / 2 AS temp_media
    FROM base
),

tendencias AS (
    SELECT
        *,
        -- Médias móveis de temperatura
        AVG(temp_media) OVER (
            PARTITION BY codigo_ibge
            ORDER BY data
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS media_movel_7d_temp,

        AVG(temp_media) OVER (
            PARTITION BY codigo_ibge
            ORDER BY data
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS media_movel_30d_temp,

        -- Tendência (diferença entre médias)
        (
          AVG(temp_media) OVER (
              PARTITION BY codigo_ibge
              ORDER BY data
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          )
          -
          AVG(temp_media) OVER (
              PARTITION BY codigo_ibge
              ORDER BY data
              ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          )
        ) AS tendencia_temp,

        -- CHUVA — acumulados
        SUM(chuva_mm) OVER (
            PARTITION BY codigo_ibge
            ORDER BY data
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS chuva_7d,

        SUM(chuva_mm) OVER (
            PARTITION BY codigo_ibge
            ORDER BY data
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS chuva_30d,

        SUM(chuva_mm) OVER (
            PARTITION BY codigo_ibge, ano, mes
        ) AS chuva_mes_total,

        -- Radiação — média móvel 7 dias
        AVG(radiacao_solar_mj_m2) OVER (
            PARTITION BY codigo_ibge
            ORDER BY data
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS radiacao_7d,

        -- Anomalias
        (temp_media -
         AVG(temp_media) OVER (
             PARTITION BY codigo_ibge
             ORDER BY data
             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
         )
        ) AS anomalia_temp,

        (chuva_mm -
         SUM(chuva_mm) OVER (
             PARTITION BY codigo_ibge
             ORDER BY data
             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
         ) / 30
        ) AS anomalia_chuva

    FROM enriquecida
)

SELECT *
FROM tendencias;


---------------------------------------------------
-- ===========================================================
-- GOLD 4 — Clima Horário Analítico
-- ===========================================================

CREATE OR REFRESH LIVE TABLE open_meteo.gold.clima_horario_analitico
COMMENT "Gold — Séries horárias por cidade com métricas derivadas"
TBLPROPERTIES ("quality" = "gold")
PARTITIONED BY (ano, mes)
AS
WITH base AS (
    SELECT
        data_hora,
        CAST(data_hora AS DATE) AS data,
        municipio,
        uf,
        latitude,
        longitude,
        temperatura_c,
        umidade_relativa,
        precipitacao_mm,
        velocidade_vento_ms,
        ano,
        mes,
        dia,
        ingested_at
    FROM open_meteo.silver.clima_horario
),

enriquecida AS (
    SELECT
        *,
        HOUR(data_hora) AS hora,

        CASE 
            WHEN HOUR(data_hora) BETWEEN 5 AND 11 THEN 'manha'
            WHEN HOUR(data_hora) BETWEEN 12 AND 17 THEN 'tarde'
            WHEN HOUR(data_hora) BETWEEN 18 AND 23 THEN 'noite'
            ELSE 'madrugada'
        END AS periodo_dia,

        CASE WHEN precipitacao_mm > 0 THEN 1 ELSE 0 END AS teve_chuva,

        CASE
            WHEN temperatura_c < 15 THEN 'frio'
            WHEN temperatura_c BETWEEN 15 AND 25 THEN 'agradavel'
            WHEN temperatura_c BETWEEN 25 AND 32 THEN 'quente'
            ELSE 'muito_quente'
        END AS classificacao_temperatura

    FROM base
)

SELECT *
FROM enriquecida;