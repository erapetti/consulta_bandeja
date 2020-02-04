/*
-- borro multas de cero horas/dias porque SIAP no las acepta
DELETE from imultas
WHERE Resultado='PE'
  AND MultFchCarga=curdate()
  AND MultCantDias=0
  AND MultCantHor=0
;
*/

-- engancho las devoluciones con la multa original usando m1.InasisLicId_Orig
UPDATE imultas m1
  JOIN imultas m2
    ON m2.InasisLicId=m1.InasisLicId_Orig
   AND m2.MultId<m1.MultId
SET m1.MultIdRef=m2.MultId
WHERE m1.Resultado='PE'
  AND m1.MultFchCarga=curdate()
  AND (m1.MultCantDias<0 OR m1.MultCantHor<0)
  AND m1.InasisLicId_Orig is not null
  AND m1.MultIdRef is null
;

-- engancho las devoluciones con la multa original usando otros datos
UPDATE imultas m1
  JOIN imultas m2
 USING (PerDocTpo,PerDocPaisCod,PerDocNum,MultCarNum,MultInsCod,MultAnio,MultMes,RubroCod)
SET m1.MultIdRef=m2.MultId
WHERE m1.Resultado='PE'
  AND m1.MultFchCarga=curdate()
  AND (m1.MultCantDias<0 OR m1.MultCantHor<0)
  AND m1.MultIdRef is null
  AND m2.MultId < m1.MultId
  AND (m1.MultCantDias<0 AND m1.MultCantDias=-m2.MultCantDias
       OR
       m1.MultCantHor<0 AND m1.MultCantHor=-m2.MultCantHor
      )
;

-- marco como error los que están excedidos
UPDATE imultas
JOIN (select perdocnum
      from imultas
      where resultado='PE'
      group by perdocnum having sum(multcantdias)+sum(multcanthor)/8 > 23*2
     )X using (perdocnum)
SET resultado='ERROR',mensaje='Exceso de horas'
WHERE resultado='PE'
;

