START TRANSACTION;

-- set @desdeBandeja='2019-04-01'; set @hastaBandeja='2019-05-01';

-- voy a hacer la liquidación del mes anterior
set @anio=if(month(@hastaBandeja)=1,-1,0)+year(@hastaBandeja);
set @mes=if(month(@hastaBandeja)=1,12,right(concat('0',month(@hastaBandeja)-1),2));

-- cantidad de semanas laborables que tiene cada mes
set @semanas=case @mes when 7 then 2.17 when 9 then 3.25 when 4 then 3.25 when 1 then 0 else 4.34 end;

-- cantidad de pesos diarios para transporte según resolución de DGI
set @diaria=12;


use siap_ces_tray;

-- Genero el envío
insert into ivariablestmp
select 'DO' PerDocTpo,
       'UY' PerDocPaisCod,
       perdocid PerDocNum,
       carnum VarCarNum,
       VarConNum,
       @anio VarAnio,
       @mes+0 VarMes,
       signo*@diaria*dias VarImporte
from
 -- los rubros: uno va con el monto positivo y el otro con el mismo monto negativo porque se tienen que netear en cero
 (select 740016 VarConNum,1 signo
  union
  select 740017 VarConNum,-1 signo
 ) Rubros

join
 -- la liquidación: persona y cantidad de días imponibles
 (select perdocid,floor(greatest(count(distinct HorarioDiaSemana),if(sum(CargaHorariaCantHoras)<20,ceil(sum(CargaHorariaCantHoras)/6.5),5))*@semanas) dias
  from
   -- los abonos emitidos en el mes
   (select perid,dependid
    from AbonosDocentes.ABONOSDEPENDDOCENTES
    where AbonoAnulado = '1000-01-01'
      and AbonoMesYear>=concat(@anio,'-',@mes,'-01')
      and AbonoViajesEmitidos>0
    group by 1,2
   ) AD
   -- busco la designación del perid
  join Personas.V_PERSONAS using (perid)
  join Personal.RELACIONES_LABORALES RL on personalperid=perid and RelLabAnulada=0
  join Personal.FUNCIONES_RELACION_LABORAL FRL USING (RELLABID)
  join Personal.FUNCIONES_ASIGNADAS FA
    on FA.funcasignadaid=FRL.funcasignadaid
   and FA.funcasignadaanulada=0
   and ifnull(FuncAsignadaFchDesde,'1000-01-01')<=concat(@anio,'-',@mes,'-31')
   and (ifnull(FuncAsignadaFchHasta,'1000-01-01')='1000-01-01' or FuncAsignadaFchHasta>=concat(@anio,'-',@mes,'-01'))
  -- busco la carga horaria por si no tiene grupos de docencia directa
  join Personal.CARGAS_HORARIAS using (CargaHorariaId)
  join Personal.SILLAS S on S.sillaid=FA.sillaid and S.silladependid=AD.dependid
  left join
   -- busco los días que trabaja en grupos de docencia directa
   (select sillaid,HorarioDiaSemana
    from
     -- elijo el grupomateria asociado a cada silla
     (select SillaId,ANY_VALUE(GrupoMateriaId) GrupoMateriaId
      from Personal.SILLAGRUPOMATERIA
      where SillaGrupoMateriaFchDesde<=concat(@anio,'-',@mes,'-31')
        and SillaGrupoMateriaFchHasta>=concat(@anio,'-',@mes,'-01')
      group by 1
     ) SGM
    join
     -- los días de la semana que ese grupomateria tiene clase
     (select GrupoMateriaId,HorarioDiaSemana
      from Estudiantil.GRUPOMATERIA_HORARIOS
      join Estudiantil.LICEOPLANTURNO_HORARIOS using (LiceoPlanDependId,LiceoPlanPlanId,TurnoId,HorarioId)
      where GrupoMateriaHorarioFchDesde<=concat(@anio,'-',@mes,'-31')
        and GrupoMateriaHorarioFchHasta>=concat(@anio,'-',@mes,'-01')
        and ifnull(HorarioFchDesde,'1000-01-01')<=concat(@anio,'-',@mes,'-31')
        and (ifnull(HorarioFchHasta,'1000-01-01')='1000-01-01' or HorarioFchHasta>=concat(@anio,'-',@mes,'-01'))
      group by 1,2
     ) GMH using (grupomateriaid)
    group by sillaid,HorarioDiaSemana
   ) DiasHorarios on DiasHorarios.sillaid=S.Sillaid
  group by perdocid
 ) Liquidacion

left join
 -- un número de cargo que se puede usar para esa persona en este mes
 (select cedula perdocid,ANY_VALUE(carnum) carnum
  from Personal.as400
  where anio=@anio
    and mes=@mes
  group by 1
 ) SIAP using (perdocid)
group by PerDocTpo,PerDocPaisCod,PerDocNum,VarCarNum,VarConNum,VarAnio,VarMes,VarImporte
;

ROLLBACK;
