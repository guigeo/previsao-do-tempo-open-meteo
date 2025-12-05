CREATE OR REFRESH LIVE TABLE open_meteo.gold.temp_mins_muns_top10
COMMENT "Gold — Temperaturas minímas nos top 10 municípios"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
  municipio,
  uf,
  concat(municipio, ' - ', uf ) as mun_uf,
  temp_min_c
FROM (
  SELECT 
    municipio,
    uf,
    temp_min_c,
    ROW_NUMBER() OVER (
      PARTITION BY municipio, uf
      ORDER BY temp_min_c ASC
    ) AS rn
  FROM 
    open_meteo.silver.clima_dia_dia
)
WHERE rn = 1
ORDER BY temp_min_c ASC
LIMIT 10;

CREATE OR REFRESH LIVE TABLE open_meteo.gold.temp_max_muns_top10
COMMENT "Gold — Temperaturas máximas nos top 10 municípios"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
  municipio,
  uf,
  concat(municipio, ' - ', uf ) as mun_uf,
  temp_max_c
FROM (
  SELECT 
    municipio,
    uf,
    temp_max_c,
    ROW_NUMBER() OVER (
      PARTITION BY municipio, uf
      ORDER BY temp_max_c DESC
    ) AS rn
  FROM 
    open_meteo.silver.clima_dia_dia
)
WHERE rn = 1
ORDER BY temp_max_c DESC
LIMIT 10;
