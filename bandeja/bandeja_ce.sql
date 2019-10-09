START TRANSACTION;

set @desdeBandeja='2019-03-01'; set @hastaBandeja='2019-08-09';

-- voy a hacer la liquidación del mes anterior
set @anio=if(month(@hastaBandeja)=1,-1,0)+year(@hastaBandeja);
set @mes=if(month(@hastaBandeja)=1,12,right(concat('0',month(@hastaBandeja)-1),2));


use siap_ces_tray;

-- LOCK TABLE Personal.COMPELECPERIODOSOPCIONES;

-- Genero el envío
insert into ivariablestmp
select 'DO' PerDocTpo,
       'UY' PerDocPaisCod,
       perdocid PerDocNum,
       carnum VarCarNum,
       643275 VarConNum,
       anio VarAnio,
       mes VarMes,
       importe VarImporte,
       CompElecPeriodoOpcionId
from (
   select perdocid,
          year(CompElecFecha) anio,
          month(CompElecFecha) mes,
          signo*(case Tipo when 'asistencia' then CompElecDineroAsistencia when 'actuacion' then CompElecDineroActuacion end) importe,
          CompElecPeriodoOpcionId
   from (

    -- las compensaciones nuevas (no enviadas y diferentes a la ultima enviada)
    select O.CompElecPeriodoId, O.PersonalPerId perid, O.Tipo, 1 signo, O.CompElecPeriodoOpcionId
    from Personal.COMPELECPERIODOSOPCIONES O
    -- es el ultimo para el periodo-persona (no hay siguiente, que seria O2)
    left join Personal.COMPELECPERIODOSOPCIONES O2
      on O2.CompElecPeriodoId = O.CompElecPeriodoId
     and O2.PersonalPerId = O.PersonalPerId
     and O2.CompElecPeriodoOpcionId > O.CompElecPeriodoOpcionId
    -- si hay uno enviado antes (luego me fijo que sea diferente compensacion)
    left join Personal.COMPELECPERIODOSOPCIONES O3
      on O3.CompElecPeriodoId = O.CompElecPeriodoId
     and O3.PersonalPerId = O.PersonalPerId
     and O3.CompElecPeriodoOpcionId < O.CompElecPeriodoOpcionId
     and O3.FechaEnvio is not null
    -- ese anterior es el ultimo enviado porque no hay una intermedia
    left join Personal.COMPELECPERIODOSOPCIONES O4
      on O4.CompElecPeriodoId = O.CompElecPeriodoId
     and O4.PersonalPerId = O.PersonalPerId
     and O4.CompElecPeriodoOpcionId < O.CompElecPeriodoOpcionId
     and O4.CompElecPeriodoOpcionId > O3.CompElecPeriodoOpcionId
     and O4.FechaEnvio is not null
    where O.Compensacion = 'dinero'
      and O.FechaEnvio is null
      and isnull(O2.CompElecPeriodoId)
      and isnull(O4.CompElecPeriodoId)
      and (isnull(O3.CompElecPeriodoOpcionId) or O.Compensacion <> O3.Compensacion or O.Tipo <> O3.Tipo)

    union

    -- las devoluciones
    select O3.CompElecPeriodoId, O3.PersonalPerId perid, O3.Tipo, -1 signo, O3.CompElecPeriodoOpcionId
    from Personal.COMPELECPERIODOSOPCIONES O
    -- es el ultimo para el periodo-persona (no hay siguiente, que seria O2)
    left join Personal.COMPELECPERIODOSOPCIONES O2
      on O2.CompElecPeriodoId = O.CompElecPeriodoId
     and O2.PersonalPerId = O.PersonalPerId
     and O2.CompElecPeriodoOpcionId > O.CompElecPeriodoOpcionId
    -- si hay una comp. de dinero enviada antes (luego me fijo que sea diferente tipo)
    left join Personal.COMPELECPERIODOSOPCIONES O3
      on O3.CompElecPeriodoId = O.CompElecPeriodoId
     and O3.PersonalPerId = O.PersonalPerId
     and O3.CompElecPeriodoOpcionId < O.CompElecPeriodoOpcionId
     and O3.FechaEnvio is not null
     and O3.Compensacion = 'dinero'
    -- esa comp. anterior es la ultima de dinero enviada porque no hay otra intermedia
    left join Personal.COMPELECPERIODOSOPCIONES O4
      on O4.CompElecPeriodoId = O.CompElecPeriodoId
     and O4.PersonalPerId = O.PersonalPerId
     and O4.CompElecPeriodoOpcionId < O.CompElecPeriodoOpcionId
     and O4.CompElecPeriodoOpcionId > O3.CompElecPeriodoOpcionId
     and O4.FechaEnvio is not null
     and O4.Compensacion = 'dinero'
    where (O.FechaEnvio is null or O.Compensacion='licencia')
      and isnull(O2.CompElecPeriodoId)
      and isnull(O4.CompElecPeriodoId)
      and (O.Compensacion <> O3.Compensacion or O.Tipo <> O3.Tipo)

   ) CAMBIOS

   join Personal.COMPELECPERIODOS P using (CompElecPeriodoId)
   join Personas.V_PERSONAS using (perid)

   where year(P.CompElecFecha) < @anio
      or (year(P.CompElecFecha) = @anio and month(P.CompElecFecha) < @mes)
      or (year(P.CompElecFecha) = @anio and month(P.CompElecFecha) = @mes and day(P.CompElecFecha) < 20)

) Liquidacion

left join (
 -- un número de cargo que se puede usar para esa persona en este mes
  select cedula perdocid,anio,mes,ANY_VALUE(carnum) carnum
  from Personal.as400
  where tipo in (2,3)
  group by 1,2,3
) SIAP using (perdocid,anio,mes)
;

use Personal;

update COMPELECPERIODOSOPCIONES
join siap_ces_tray.ivariablestmp using (CompElecPeriodoOpcionId)
set FechaEnvio=now()
;

ROLLBACK;
