START TRANSACTION;

-- set @desdeBandeja='2019-04-01'; set @hastaBandeja='2019-05-01';

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
WHERE InasisLicFecha >= @desdeBandeja
  AND InasisLicFecha < @hastaBandeja
  AND InasisLicFchIni = InasisLicFchFin
  AND InasisLicEstado in ('P','R')
  AND InasisLicTipo='DD'
  AND InasCausDescuento<>0
  AND InasCausId<>'ATD'
  AND InasCausTipo='I'
;

-- Marco como rebotados los que no encuentro en la tabla as400
UPDATE INASISLIC
JOIN FUNCIONES_ASIGNADAS USING (FuncAsignadaId)
JOIN SILLAS USING (SillaId)
JOIN Personas.PERSONASDOCUMENTOS ON personalperid=perid AND paiscod='UY' AND doccod='CI'
LEFT JOIN FUNCIONES_RELACION_LABORAL USING (FuncAsignadaId)
LEFT JOIN RELACIONES_LABORALES RL USING (RelLabId,PersonalPerId,SillaId)
LEFT JOIN (
           select DependId,cedula,anio,mes,Ciccod
           from as400
           where anio>=year(@desdeBandeja)
             and anio<=year(@hastaBandeja)
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
     (SELECT SillaDependId DependID,
             a.InsCod,
             a.CarNum,
             a.AsiCod,
             a.Caracter,
             PERSONALPERID,
             perdocid,
             if(InasisLicCicloPago is not null,InasisLicCicloPago,RelLabCicloPago) CicloPago,
             CargaHorariaCantHoras HorasPorSemana,
             InasisLicId,
             InasisLicFchIni,
             InasisLicId_Orig,
             InasCausId,
             InasisLicTipoMov='E' ENMIENDA,
             sum(if(InasisLicDiaHora='H',InasisLicCant,0)) horas,
             sum(if(InasisLicDiaHora='D',1,0)) dias
      FROM Personal.INASISLIC
      JOIN Personal.INASCAUSALES USING (InasCausId)
      JOIN Personal.FUNCIONES_ASIGNADAS USING (FuncAsignadaId)
      JOIN Personal.SILLAS USING (SillaId)
      JOIN Personal.CARGAS_HORARIAS USING (CargaHorariaId)
      JOIN Personas.PERSONASDOCUMENTOS ON personalperid=perid AND paiscod='UY' AND doccod='CI'
      LEFT JOIN Personal.FUNCIONES_RELACION_LABORAL USING (FuncAsignadaId)
      LEFT JOIN Personal.RELACIONES_LABORALES RL USING (RelLabId,PersonalPerId)
      LEFT JOIN (
           select DependId,cedula,anio,mes,Ciccod,ANY_VALUE(InsCod) InsCod,ANY_VALUE(CarNum) CarNum,ANY_VALUE(AsiCod) AsiCod,ANY_VALUE(Caracter) Caracter
           from Personal.as400
           where anio>=year(@desdeBandeja)
             and anio<=year(@hastaBandeja)
             and tipo=3
           group by 1,2,3,4,5
      ) a on a.DependId=SillaDependId and a.cedula=cast(perdocid as unsigned) and a.anio=year(InasisLicFchIni) and a.mes=month(InasisLicFchIni) and a.CicCod=if(InasisLicCicloPago is not null,InasisLicCicloPago,RelLabCicloPago)
      WHERE InasisLicEstado='X'
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
      ) I
   LEFT JOIN (
            SELECT LiceoPlanDependId DependId
            FROM Estudiantil.LICEOPLANTURNO_HORARIOS
            WHERE HorarioDiaSemana='SAB'
              AND (HorarioFchHasta IS NULL OR HorarioFchHasta >= @desdeBandeja)
              AND (HorarioFchDesde IS NULL OR HorarioFchDesde < @hastaBandeja)

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
