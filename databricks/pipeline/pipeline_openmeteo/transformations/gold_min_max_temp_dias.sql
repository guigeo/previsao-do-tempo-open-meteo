CREATE OR REFRESH LIVE TABLE open_meteo.gold.temp_mins_dias_top10
COMMENT "Gold — Temperaturas minímas nos top 10 dias"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
  data,
  temp_min_c
FROM (
  SELECT 
    data,
    temp_min_c,
    ROW_NUMBER() OVER (
      PARTITION BY data
      ORDER BY temp_min_c ASC
    ) AS rn
  FROM 
    open_meteo.silver.clima_dia_dia
)
WHERE rn = 1
ORDER BY temp_min_c ASC
LIMIT 10;

CREATE OR REFRESH LIVE TABLE open_meteo.gold.temp_max_dias_top10
COMMENT "Gold — Temperaturas máximas nos top 10 dias"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
  data,
  temp_max_c
FROM (
  SELECT 
    data,
    temp_max_c,
    ROW_NUMBER() OVER (
      PARTITION BY data
      ORDER BY temp_max_c DESC
    ) AS rn
  FROM 
    open_meteo.silver.clima_dia_dia
)
WHERE rn = 1
ORDER BY temp_max_c DESC
LIMIT 10;
