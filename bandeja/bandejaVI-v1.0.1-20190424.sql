start transaction;

use Personal;

/*
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
set FuncionAsignadaCompEstado='X',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'P'
and TipoCompId = 2;
*/


use siap_ces_tray;

insert into iviaticos
select  null ViatId,
        DocCod PerDocTpo,
        PaisCod PerDocPaisCod,
        PerDocId PerDocNum,
        733481 CodConcepto,
        FuncionAsignadaCompAnio ViatAnio,
        FuncionAsignadaCompMes ViatMes,
        if(FuncionAsignadaCompTipoMov='E',-1,1)*FuncionAsignadaCompCantidad ViatCant,
        1 ViaticoComun,
        curdate() ViatFchCarga,
        'PE' ViatResultado,
        '' ViatMensaje,
        null ViatFchProc,
        null ViatNroLote,
        null ViatIdRef
from Personal.FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
join Personas.V_PERSONAS on PerId=PersonalPerId
Where FuncionAsignadaCompEstado = 'P' /* 'X' */
and TipoCompId = 2;

/*
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
set FuncionAsignadaCompEstado='E',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'X'
and TipoCompId = 2;
*/

commit;
