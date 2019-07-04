
-- ----------------------------------------------------------------------------------------------------------------------------------
-- -- Las dependencias que registraron inasistencias tienen que existir en SIAP (tabla as400)


select 'Registros de inasistencias en dependencias que no existen en SIAP',ifnull(sum(a.dependid is null),0) cant
from (
  SELECT silladependid dependid,year(InasisLicFchIni) anio,month(InasisLicFchIni) mes
  FROM INASISLIC
  JOIN INASCAUSALES USING (InasCausId)
  JOIN FUNCIONES_ASIGNADAS USING (FUNCASIGNADAID)
  JOIN SILLAS USING (SILLAID)
  JOIN Direcciones.DEPENDENCIAS D ON D.dependid=silladependid
  WHERE InasisLicEstado='P'
    AND InasCausDescuento<>0
    AND InasCausTipo='I'
    AND InasCausDescuento=1
    AND DATE_SUB(CONCAT(YEAR(CURDATE()),'-',MONTH(CURDATE()),'-01'), interval 6 month) <= InasisLicFchIni
    AND CONCAT(YEAR(CURDATE()),'-',MONTH(CURDATE()),'-01') > InasisLicFchIni
  GROUP BY 1,2,3
) X
LEFT JOIN as400 a USING (dependid,anio,mes)
GROUP BY 1;

