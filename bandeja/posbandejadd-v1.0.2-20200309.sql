-- Borro registros posteriores al último mes del período
delete ihc
from ihorasclase ihc
join (select date_add(concat(year(hasta),'-',month(hasta),'-',1),interval 1 month) hasta
      from (select max(hasta) hasta from siap_ces_tray.periodos where bandeja='dd')Xi
     ) P
where DesFchProc is null
  and DesFchCarga = curdate()
  and (HorClaFchCese < HorClaFchPos or horclafchpos>=P.hasta)
  and (@ci = '' or PerDocNum = @ci);


-- Ingreso bajas en dependencias donde ya no tiene horas
insert into ihorasclase
(HorClaId,PerDocTpo,PerDocPaisCod,PerDocNum,HorClaCarNumVer,HorClaFchIng,HorClaInsCod,HorClaCurTpo,HorClaCur,HorClaArea,HorClaAnio,HorClaGrupo,HorClaHorTope,HorClaCar,HorClaFchPos,HorClaFchCese,HorClaObs,HorClaNumInt,HorClaParPreCod,HorClaCompPor,HorClaLote,HorClaAudUsu,HorClaFchLib,HorClaCauBajCod,HorClaAsiCod,HorClaMod,HorClaHor,HorClaBajLog,HorClaCic,HorClaEmpCod,DesFchProc,Resultado,Mensaje,HorClaCarNum,DesFchCarga,NroLote)

select null HorClaId,
       HC1.PerDocTpo,HC1.PerDocPaisCod,HC1.PerDocNum,HC1.HorClaCarNumVer,HC1.HorClaFchIng,HC1.HorClaInsCod,HC1.HorClaCurTpo,HC1.HorClaCur,HC1.HorClaArea,HC1.HorClaAnio,HC1.HorClaGrupo,HC1.HorClaHorTope,HC1.HorClaCar,HC1.HorClaFchPos,HC1.HorClaFchCese,HC1.HorClaObs,HC1.HorClaNumInt,HC1.HorClaParPreCod,HC1.HorClaCompPor,HC1.HorClaLote,HC1.HorClaAudUsu,HC1.HorClaFchLib,
       99 HorClaCauBajCod,
       HC1.HorClaAsiCod,HC1.HorClaMod,HC1.HorClaHor,
       1 HorClaBajLog,
       HC1.HorClaCic,HC1.HorClaEmpCod,
       null DesFchProc,
       'PE' Resultado,
       '' Mensaje,
       HC1.HorClaCarNum,
       curdate() DesFchCarga,
       null NroLote
from ihorasclase HC1
left join ihorasclase HC2
  on HC2.perdocnum=HC1.perdocnum
 and HC2.desfchcarga=curdate()
 and HC2.horclainscod=HC1.horclainscod
 and HC2.DesFchProc is null
where (HC1.HorClaBajLog=0 or HC1.HorClaBajLog is null)
  and HC1.Resultado='OK'
  and HC1.DesFchCarga>=concat(year(curdate())-if(month(curdate())<3,1,0),'-03-01')
  and HC2.perdocnum is null
  and (HC1.perdocnum = @ci or (HC1.perdocnum in (select perdocnum from ihorasclase where desfchcarga=curdate() and DesFchProc is null and (HorClaBajLog=0 or HorClaBajLog is null) group by 1)));


-- horas de docentes PROCES ANEP
delete from ihorasclase
where DesFchProc is null
and DesFchCarga = curdate()
 and horclainscod="25.1.0.0.8007"
 and horclaasicod<>151;

