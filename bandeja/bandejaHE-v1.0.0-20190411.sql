start transaction;

use Personal;

/*
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
set FuncionAsignadaCompEstado='X',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'P'
and TipoCompId = 1;
*/


use siap_ces_tray;

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
Where FuncionAsignadaCompEstado = 'P' /* 'X' */
and TipoCompId = 1;

use Personal;
/*
update FUNCION_ASIGNADAS_COMPENSACIONES_MENSUAL
set FuncionAsignadaCompEstado='E',
    FuncionAsignadaCompEnvio=curdate()
where FuncionAsignadaCompEstado = 'X'
and TipoCompId = 1;
*/

commit;
