sqlite3 data/clima.db

.headers on
.mode column

select data, count (*) from clima_raw group by data order by data;

SELECT * FROM clima_raw WHERE data = '2025-09-17';

DELETE FROM clima_raw WHERE data = '2025-09-18';
