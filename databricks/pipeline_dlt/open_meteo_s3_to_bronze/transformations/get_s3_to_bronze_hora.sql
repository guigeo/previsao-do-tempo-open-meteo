CREATE OR REFRESH STREAMING LIVE TABLE open_meteo.bronze.clima_horario
COMMENT "Bronze — clima horario a partir de raw s3"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
    CAST(data_hora AS STRING)           AS data_hora,
    CAST(municipio AS STRING)           AS municipio,
    CAST(uf AS STRING)                  AS uf,

    CAST(latitude AS STRING)            AS latitude,
    CAST(longitude AS STRING)           AS longitude,

    CAST(temperatura_c AS STRING)       AS temperatura_c,
    CAST(umidade_relativa AS STRING)    AS umidade_relativa,
    CAST(precipitacao_mm AS STRING)     AS precipitacao_mm,
    CAST(velocidade_vento_ms AS STRING) AS velocidade_vento_ms,
    current_timestamp()               AS ingested_at
FROM cloud_files(
  "${input_path_horario}",
  "parquet",
  map(
    "header", "true",
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaEvolutionMode", "addNewColumns",
    "pathGlobFilter", "*.parquet"
  )
);    