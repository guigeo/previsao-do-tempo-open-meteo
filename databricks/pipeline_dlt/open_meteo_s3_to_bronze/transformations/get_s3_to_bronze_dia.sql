CREATE OR REFRESH STREAMING LIVE TABLE open_meteo.bronze.clima_diario
COMMENT "Bronze — clima diario a partir de raw s3"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
    CAST(data AS STRING) AS data,
    CAST(codigo_ibge AS STRING) AS codigo_ibge,
    CAST(nome AS STRING) AS municipio,
    CAST(nome_uf AS STRING) AS uf,
    CAST(latitude AS STRING) AS latitude,
    CAST(longitude AS STRING) AS longitude,

    CAST(temp_max_c AS STRING) AS temp_max_c,
    CAST(temp_min_c AS STRING) AS temp_min_c,

    CAST(sensacao_termica_max_c AS STRING) AS sensacao_termica_max_c,
    CAST(sensacao_termica_min_c AS STRING) AS sensacao_termica_min_c,

    CAST(precipitacao_total_mm AS STRING) AS precipitacao_total_mm,
    CAST(chuva_mm AS STRING) AS chuva_mm,
    CAST(neve_mm AS STRING) AS neve_mm,

    CAST(vento_velocidade_max_kmh AS STRING) AS vento_velocidade_max_kmh,
    CAST(rajadas_vento_max_kmh AS STRING) AS rajadas_vento_max_kmh,
    CAST(vento_direcao_dominante_graus AS STRING) AS vento_direcao_dominante_graus,

    CAST(radiacao_solar_mj_m2 AS STRING) AS radiacao_solar_mj_m2,

    CAST(codigo_tempo_wmo AS STRING) AS codigo_tempo_wmo,
    current_timestamp()               AS ingested_at
FROM cloud_files(
  "${input_path_diario}",
  "parquet",
  map(
    "header", "true",
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaEvolutionMode", "addNewColumns",
    "pathGlobFilter", "*.parquet"
  )
);    