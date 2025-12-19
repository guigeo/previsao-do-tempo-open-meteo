CREATE OR REFRESH LIVE TABLE open_meteo.gold.media_temp_chuva_muns
COMMENT "Gold — Media de temperatura e chuvas em mm por município"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT a.mun_uf,
       a.uf,
       round(a.media_chuva_mm, 2) as media_chuva_mm,
       round(b.media_temperatura, 2) as media_temperatura
FROM (
SELECT 
municipio,
uf,
concat(municipio, ' - ', uf ) as mun_uf,
AVG(chuva_mm) media_chuva_mm
FROM 
open_meteo.silver.clima_dia_dia
group by municipio,
uf) a
join (
select 
mun_uf,
uf,
AVG(media_temperatura_dia) as media_temperatura
FROM (
select 
ano,
mes,
dia,
municipio,
uf,
concat(municipio, ' - ', uf ) as mun_uf,
AVG(temperatura_c) as media_temperatura_dia
from open_meteo.silver.clima_hora_hora
group by 
ano,
mes,
dia,
municipio,
uf)
group by 
mun_uf, uf) b
on a.mun_uf = b.mun_uf;