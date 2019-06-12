start transaction;

use Personal;

-- si había alguna colgada de algún proceso anterior entonces la marco como rebotada
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
set FuncionAsignadaCompEstado='R',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'X';

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
  and TipoCompId = 2;


use siap_ces_tray;

-- realizo la liquidación:
insert into iviaticos
select  null ViatId,
        DocCod PerDocTpo,
        PaisCod PerDocPaisCod,
        PerDocId PerDocNum,
        733481 CodConcepto,
        FuncionAsignadaCompAnio ViatAnio,
        FuncionAsignadaCompMes ViatMes,
        sum(if(FuncionAsignadaCompTipoMov='E',-1,1)*FuncionAsignadaCompCantidad) ViatCant,
        Compensaciones_Configuracion_Tipo='CO' ViaticoComun,
        curdate() ViatFchCarga,
        'PE' Resultado,
        '' Mensaje,
        null ViatFchProc,
        null NroLote,
        null ViatIdRef
from Personal.FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
join Personal.FUNCION_ASIGNADAS_COMPENSACIONES using (FuncionAsignadaCompId,TipoCompId,PersonalPerId)
join Personas.V_PERSONAS on PerId=PersonalPerId
where FuncionAsignadaCompEstado = 'X'
group by 1,2,3,4,5,6,7,9,10,11,12,13,14,15;

use Personal;

-- marco los procesados como enviadas
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
join Personas.V_PERSONAS on PerId=PersonalPerId
set FuncionAsignadaCompEstado='E',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'X';

commit;
