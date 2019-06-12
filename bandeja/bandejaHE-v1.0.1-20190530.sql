start transaction;

use Personal;

-- selecciono los registros con los cuales voy a trabajar en la liquidación:
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
set FuncionAsignadaCompEstado='X',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'P'
  -- considero los registros desde abril/2019 que es cuando empezó este sistema
  and (FuncionAsignadaCompAnio>2019 or FuncionAsignadaCompAnio=2019 and FuncionAsignadaCompMes>3)
  -- considero los registros de hasta el mes pasado
  and (FuncionAsignadaCompAnio < year(curdate())
       or FuncionAsignadaCompAnio = year(curdate())
          and FuncionAsignadaCompMes < month(curdate())
      )
  and TipoCompId = 1;


use siap_ces_tray;

-- realizo la liquidación:
insert into ihorasextras
select  null HorasId,
        DocCod PerDocTpo,
        PaisCod PerDocPaisCod,
        PerDocId PerDocNum,
        613013 CodConcepto,
        FuncionAsignadaCompAnio HorasAnio,
        right(concat('0',FuncionAsignadaCompMes),2) HorasMes,
        if(FuncionAsignadaCompTipoMov='E',-1,1)*FuncionAsignadaCompCantidad HorExtrasCant,
        curdate() HorasFchCarga,
        'PE' Resultado,
        '' Mensaje,
        null HorasFchProc,
        null HorasNroLote,
        null HorasIdRef
from Personal.FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
join Personas.V_PERSONAS on PerId=PersonalPerId
Where FuncionAsignadaCompEstado = 'X'
and TipoCompId = 1;


use Personal;

-- marco los procesados como enviadas
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
join Personas.V_PERSONAS on PerId=PersonalPerId
set FuncionAsignadaCompEstado='E',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'X'
and TipoCompId = 1;

-- si quedó alguna la marco como rebotada (es igual al anterior pero sin join)
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
set FuncionAsignadaCompEstado='R',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'X'
and TipoCompId = 1;

commit;
