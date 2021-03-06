START TRANSACTION;

-- set @desdeBandeja='2019-08-16'; set @hastaBandeja='2019-09-15'; set @modo='TODO';

use Personal;

-- Marco como rebotados los que están como X de alguna corrida anterior
UPDATE INASISLIC
SET InasisLicEstado='R'
WHERE InasisLicEstado='X'
;

-- Marco los registros que tengo que considerar para el envío
UPDATE INASISLIC
JOIN INASCAUSALES USING (InasCausId)
JOIN FUNCIONES_ASIGNADAS USING (FuncAsignadaId)
JOIN SILLAS USING (SillaId)
JOIN multas_cierre m ON m.DependId=SillaDependId AND m.anio=year(InasisLicFchIni) AND m.mes=month(InasisLicFchIni)
SET InasisLicFecEnvio=curdate(),InasisLicEstado='X'
WHERE InasisLicEstado in ('P','R')
  AND InasisLicFchIni = InasisLicFchFin
  AND InasisLicTipo='DD'
  AND InasCausDescuento<>0
  AND InasCausId<>'ATD'
  AND InasCausTipo='I'
  AND (@modo='TODO' OR InasisLicTipoMov='E')
  AND (InasisLicFchIni>date_sub(curdate(),interval 5 month) or inasislicid in (1549152,1549155,1549156))
;

set @desde=(select min(InasisLicFchIni) from INASISLIC where InasisLicEstado='X');
set @hasta=(select max(InasisLicFchIni) from INASISLIC where InasisLicEstado='X');


-- Marco como rebotados los que no encuentro en la tabla as400
UPDATE INASISLIC
JOIN FUNCIONES_ASIGNADAS USING (FuncAsignadaId)
LEFT JOIN FUNCIONES_RELACION_LABORAL FRL USING (FuncAsignadaId)
LEFT JOIN RELACIONES_LABORALES RL USING (RelLabId,PersonalPerId)
LEFT JOIN SILLAS SRL ON SRL.SillaId=RL.SillaId
LEFT JOIN Personas.PERSONASDOCUMENTOS ON perid=personalperid AND paiscod='UY' AND doccod='CI'
LEFT JOIN (
           select DependId,cedula,anio,mes,Ciccod
           from as400
           where (anio=year(@desde) and mes>=month(@desde)
                  or anio>year(@desde)
                 )
             and tipo=3
           group by 1,2,3,4,5
) a on a.DependId=SillaDependId and a.cedula=cast(perdocid as unsigned) and a.anio=year(InasisLicFchIni) and a.mes=month(InasisLicFchIni) and a.CicCod=if(InasisLicCicloPago is not null,InasisLicCicloPago,RelLabCicloPago)
SET InasisLicEstado='R'
WHERE InasisLicEstado='X'
  AND a.cedula is null
;

use siap_ces_tray;

-- Genero el envío
insert into imultastmp
SELECT
   "DO" PerDocTpo,
   "UY" PerDocPaisCod,
   CEDULA PerDocNum,
   CarNum MultCarNum,
   RUBRO RubroCod,
   year(InasisLicFchIni) MultAnio,
   month(InasisLicFchIni) MultMes,
   CICLO MultCic,
   CARACTER MultCar,
   InsCod MultInsCod,
   if(AsiCod="",null,AsiCod) MultAsiCod,
   0 MultCantDias,
   if(ENMIENDA,-1,1)*HORAS MultCantHor,
   0 MultCantMin,
   InasisLicId,
   InasisLicId_Orig
FROM
 ( SELECT
    InsCod,
    CarNum,
    AsiCod,
    Caracter,
    perdocid CEDULA,
    ifnull(CicloPago,0) CICLO,
    if(InasCausId<>'MG','81118','381118')  RUBRO,
    (floor( sum(dias) * (HorasPorSemana/if(SABADO.DependId is null,5,6)) ) + sum(horas)) * if(InasCausId='SA',2,1) HORAS,
    ENMIENDA,
    I.InasisLicId,
    InasisLicFchIni,
    InasisLicId_Orig

   FROM
     (SELECT DependID,
             InsCod,
             CarNum,
             AsiCod,
             Caracter,
             PERSONALPERID,
             perdocid,
             CicCod CicloPago,
             CargaHorariaCantHoras HorasPorSemana,
             InasisLicId,
             InasisLicFchIni,
             InasisLicId_Orig,
             InasCausId,
             InasisLicTipoMov='E' ENMIENDA,
             sum(if(InasisLicDiaHora='H',InasisLicCant,0)) horas,
             sum(if(InasisLicDiaHora='D',1,0)) dias
      FROM 
        (SELECT INASISLIC.*,ANY_VALUE(CargaHorariaCantHoras) CargaHorariaCantHoras,ANY_VALUE(a.DependId) DependId,ANY_VALUE(a.CicCod) CicCod,ANY_VALUE(a.InsCod) InsCod,ANY_VALUE(a.CarNum) CarNum,ANY_VALUE(a.AsiCod) AsiCod,ANY_VALUE(a.Caracter) Caracter,ANY_VALUE(perdocid) perdocid
         FROM Personal.INASISLIC
         JOIN Personal.FUNCIONES_ASIGNADAS USING (FuncAsignadaId)
         JOIN Personal.CARGAS_HORARIAS USING (CargaHorariaId)
         JOIN Personal.FUNCIONES_RELACION_LABORAL USING (FuncAsignadaId)
         JOIN Personal.RELACIONES_LABORALES RL USING (RelLabId,PersonalPerId)
         JOIN Personal.SILLAS SRL ON SRL.SillaId=RL.SillaId
         JOIN Personas.PERSONASDOCUMENTOS ON personalperid=perid AND paiscod='UY' AND doccod='CI'
         JOIN (
              select DependId,cedula,anio,mes,Ciccod,ANY_VALUE(InsCod) InsCod,ANY_VALUE(CarNum) CarNum,ANY_VALUE(AsiCod) AsiCod,ANY_VALUE(Caracter) Caracter
              from Personal.as400
              where (anio=year(@desde) and mes>=month(@desde)
                     or anio>year(@desde)
                    )
                and tipo=3
              group by 1,2,3,4,5
         ) a on a.DependId=SillaDependId and a.cedula=cast(perdocid as unsigned) and a.anio=year(InasisLicFchIni) and a.mes=month(InasisLicFchIni) and a.CicCod=if(InasisLicCicloPago is not null,InasisLicCicloPago,RelLabCicloPago)
         WHERE InasisLicEstado='X'
           AND CargaHorariaCantHoras>0
         GROUP BY InasisLicId,FuncAsignadaId,InasisLicIdentificador
        ) IRL
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
      ) I
   LEFT JOIN (
            SELECT LiceoPlanDependId DependId
            FROM Estudiantil.LICEOPLANTURNO_HORARIOS
            WHERE HorarioDiaSemana='SAB'
              AND (HorarioFchHasta IS NULL OR HorarioFchHasta >= @desde)
              AND (HorarioFchDesde IS NULL OR HorarioFchDesde <= @hasta)
            GROUP BY 1
   ) SABADO USING (DependId)

   GROUP BY InsCod,
            CarNum,
            AsiCod,
            Caracter,
            CEDULA,
            CICLO,
            RUBRO,
            InasCausId,
            HorasPorSemana,
            SABADO.DependId,
            ENMIENDA,
            InasisLicId,
            InasisLicFchIni,
            InasisLicId_Orig
)ABYVHDOC
FOR UPDATE
;

use Personal;

-- Actualizo los registros marcados como que ahora están enviados
UPDATE INASISLIC
JOIN INASCAUSALES USING (InasCausId)
JOIN FUNCIONES_ASIGNADAS USING (FuncAsignadaId)
JOIN SILLAS USING (SillaId)
JOIN CARGAS_HORARIAS USING (CargaHorariaId)
JOIN Personas.PERSONASDOCUMENTOS ON personalperid=perid AND paiscod='UY' AND doccod='CI'
SET InasisLicEstado='E'
WHERE InasisLicEstado='X'
;

-- Marco como rebotados los que quedaron con X
UPDATE INASISLIC
SET InasisLicEstado='R'
WHERE InasisLicEstado='X'
;

ROLLBACK;
