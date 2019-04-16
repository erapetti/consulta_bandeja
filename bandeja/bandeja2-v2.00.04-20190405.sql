-- ----------------------------------------------------------------------------------------------------------------------------------
-- -- Todas las designaicones Interinas y Suplentes sin fecha de registro de cese, 
-- -- se actualizara con la fecha de registro del alta de la designacion

--  ********************************** ORDEN DE BANDEJA -------------------------------------------------------------
--  ********************************** -------------------------------------------------------------



use siap_ces_tray;

-- Variables para el RANGO DE PERIODO DE  BANDEJA, en producción se reemplazan por otros valores
set @desdeBandeja='2018-08-09';
set @hastaBandeja ='2019-04-03';

insert into idesignaciones
select DesigId,EmpCod,PerDocTpo,PerDocPaisCod,PerDocNum,CarNum,RelLabId,CarCod,CarNumVer,CarNiv,EscCod,CatCod,DesFchIng,DesFchEgr,FunCod,DesActLabNum,SegSalCod,LiqUniCod,VinFunCod,CauBajCod,ComEspCod,TipoRemCod,DesInsCod,DesInsFchDesde,DesInsFchHasta,CarRegHor,DesRegHor,DesRegHorFchDesde,DeRegHorFchHasta,DesCarId,DesCarCod,ForAccCarCod,DesGraEsp,DesFchFinCon,DesObs,DesFchIniCon,SitFunId,SitFunFchDesde,SitFunFchHasta,LiqGruCod,DesFunCod,DesFchCarga,DesFchProc,Resultado,Mensaje,NroLote
from (




-- ********************************** ALTAS -------------------------------------------------------------
--  ********************************** ------------------------------------------------------------------
(SELECT null DesigId,
1 EmpCod,'DO' PerDocTpo,'UY' PerDocPaisCod,CONVERT(Perdocid,char(24)) PerDocNum,if(Rellabdesigncaracter='S', null,A.Correlativo) CarNum,A.rellabid RelLabId,
DenomCargoSiapId CarCod,Null CarNumVer,NivelCargo CarNiv,'H' EscCod,
if ((if(Rellabdesigncaracter='S', null,A.Correlativo)) is not null,2,5) CatCod,
IniActividades DesFchIng,RelLabCeseFchReal DesFchEgr,A.Personalperid FunCod,
2 DesActLabNum,null SegSalCod,'1.2' LiqUniCod,12 VinFunCod,Caubajcod CauBajCod,21 ComEspCod,Null TipoRemCod,DEP_DBC DesInsCod,IniActividades DesInsFchDesde,RelLabCeseFchReal DesInsFchHasta,
CargaHorariaCantHoras CarRegHor,C.horastotales2 DesRegHor,IniActividades DesRegHorFchDesde,RelLabCeseFchReal DeRegHorFchHasta,Null DesCarId,
(case Rellabdesigncaracter when 'E' then 1 when 'I' then 2 else 3 end) DesCarCod,
Null ForAccCarCod,Null DesGraEsp,Null DesFchFinCon,'' DesObs,null DesFchIniCon, 
1 SitFunId,IniActividades SitFunFchDesde,RelLabCeseFchReal SitFunFchHasta,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
,FuncAsignadaFchAlta BandejaOrden
FROM
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,rellabfchfinprevista,
 PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, DenomCargoSiapId,NivelCargo
,FuncAsignadaFchAlta
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoCesId = DENOMINACIONES_CARGOS.DenomCargoId
where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and P.denomcargoid in(3000,3100,858,859,420,410,411,12356,12355,12354,12353,12352,12360,12361,100022)
and (V.FuncionId = PuestoFuncionId or V.FuncionId = 100006)

and (RL.RelLabFchIniActividades >= '2019-03-01') 

-- RANGO DE PERIODO DE  BANDEJA ************** ----------------------------------------
and (FA.FuncAsignadaFchAlta >= @desdeBandeja and FA.FuncAsignadaFchAlta < @hastaBandeja)

and (FA.FuncAsignadaFchDesde >= '2019-03-01')

 and (
     (DENOMINACIONES_CARGOS.DenomCargoId in(3000,3100) and(ifnull(RelLabDesignCatLiceo,0) = 0 or DCSC.Categoria = RelLabDesignCatLiceo ))
     or
     (DENOMINACIONES_CARGOS.DenomCargoId not in(3000,3100) and DCSC.Categoria is null)
 )

) A 

left join 
(
select RelLabId,Sum(FuncRelLabCantHrs) horastotales2, FA.FuncAsignadaFchDesde desde, FA.FuncAsignadaFchHasta hasta
from Personal.FUNCIONES_RELACION_LABORAL FRL
join Personal.FUNCIONES_ASIGNADAS FA using (funcasignadaid)
where FuncAsignadaAnulada=0
group by FRL.RelLabId,desde,hasta
) C
on A.rellabid = C.rellabid
and A.IniActividades = desde
and (hasta is null or A.IniActividades <= hasta)

group by RelLabId
)

UNION

-- ********************************** CESES (solo designacion no FAsignadas)-------------------------------------------------------------
--  ********************************** ------------------------------------------------------------------
(select null DesigId,
1,'DO','UY',Perdocid,if(Rellabdesigncaracter='S', null,A.Correlativo),A.rellabid,
DenomCargoSiapId,Null,NivelCargo,'H',
if ((if(Rellabdesigncaracter='S', null,A.Correlativo)) is not null,2,5),
IniActividades,RelLabCeseFchReal,A.Personalperid,
2,null,'1.2',12,Caubajcod,21,Null,DEP_DBC,IniActividades,RelLabCeseFchReal,
CargaHorariaCantHoras,CargaHorariaCantHoras,IniActividades,RelLabCeseFchReal,Null,
(case Rellabdesigncaracter when 'E' then 1 when 'I' then 2 else 3 end),
Null ForAccCarCod,Null DesGraEsp,Null DesFchFinCon,'' DesObs,null DesFchIniCon, 
-- 8,IniActividades,RelLabCeseFchReal,
null,null,null,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
,RelLabCeseFchAlta BandejaOrden
from
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,
rellabfchfinprevista, RelLabCeseFchAlta,P.Correlativo,DEP_DBC,DenomCargoSiapId,NivelCargo
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoCesId = DENOMINACIONES_CARGOS.DenomCargoId
where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and P.denomcargoid in(3000,3100,858,859,420,410,411,12356,12355,12354,12353,12352,12360,12361,100022)
and V.FuncionId = PuestoFuncionId
-- Fecha de Registro del CESE de la Relacion Laboral -* DESDE -----------------------
-- and RL.RelLabCeseFchAlta > '2018-08-09' 

-- RANGO DE PERIODO DE  BANDEJA ************** ----------------------------------------
-- En la negacion de las altas (abajo) para el periodo de la bandeja, 
-- no se toma en cuenta la fecha de registro de funcion asignada, se usa la de la Relacion Laboral
-- (RelLabDesignFchAlta)
-- como si se hace en las Altas (donde se toma en cuenta el + 8 para Directores etc.)
-- Resumen no se toma en cuenta el + 8 de Fucnion asignda para Directores etc.

 and (
     (DENOMINACIONES_CARGOS.DenomCargoId in(3000,3100) and(ifnull(RelLabDesignCatLiceo,0) = 0 or DCSC.Categoria = RelLabDesignCatLiceo ))
     or
     (DENOMINACIONES_CARGOS.DenomCargoId not in(3000,3100) and DCSC.Categoria is null)
 )

and (RL.RelLabCeseFchAlta  >= @desdeBandeja and RL.RelLabCeseFchAlta < @hastaBandeja)
and not (RL.RelLabDesignFchAlta >= @desdeBandeja and RL.RelLabDesignFchAlta < @hastaBandeja)

 and
 (( 
 RL.RelLabCeseFchReal >= '2019-03-01'
 and (RL.RelLabFchIniActividades >= '2019-03-01' and RL.RelLabCeseFchAlta <> RL.RelLabDesignFchAlta)
 ))
 and (
     (DENOMINACIONES_CARGOS.DenomCargoId in(3000,3100) and(ifnull(RelLabDesignCatLiceo,0) = 0 or DCSC.Categoria = RelLabDesignCatLiceo ))
     or
     (DENOMINACIONES_CARGOS.DenomCargoId not in(3000,3100) and DCSC.Categoria is null)
 )

)A
)

UNION

-- ********************************** ANULACIONES -------------------------------------------------------------
--  ********************************** ------------------------------------------------------------------

(select null DesigId,
1 EmpCod,'DO' PerDocTpo,'UY' PerDocPaisCod,CONVERT(Perdocid,char(24)) PerDocNum,if(Rellabdesigncaracter='S', null,A.Correlativo) CarNum,A.rellabid RelLabId,
DenomCargoSiapId CarCod,Null CarNumVer,NivelCargo CarNiv,'H' EscCod,
if ((if(Rellabdesigncaracter='S', null,A.Correlativo)) is not null,2,5) CatCod,
A_RelLabFchIniActividades DesFchIng,A_RelLabFchIniActividades DesFchEgr,A.Personalperid FunCod,
2 DesActLabNum,null SegSalCod,'1.2' LiqUniCod,12 VinFunCod,99 CauBajCod,21 ComEspCod,Null TipoRemCod,DEP_DBC DesInsCod,A_RelLabFchIniActividades DesInsFchDesde,A_RelLabFchIniActividades DesInsFchHasta,
CargaHorariaCantHoras CarRegHor,CargaHorariaCantHoras DesRegHor,A_RelLabFchIniActividades DesRegHorFchDesde,A_RelLabFchIniActividades DeRegHorFchHasta,Null DesCarId,
(case Rellabdesigncaracter when 'E' then 1 when 'I' then 2 else 3 end) DesCarCod,
Null ForAccCarCod,Null DesGraEsp,Null DesFchFinCon,'' DesObs,null DesFchIniCon, 
-- 8 SitFunId,A_RelLabFchIniActividades SitFunFchDesde,null SitFunFchHasta,
null SitFunId, null SitFunFchDesde,null SitFunFchHasta,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
, AnulacionFchAlta BandejaOrden
FROM
(SELECT 
AnulacionValorPkTabla,
JSON_UNQUOTE(JSON_EXTRACT(AnulacionDatos,'$.RelLabCeseFchReal')) A_RelLabCeseFchReal, 
JSON_UNQUOTE(JSON_EXTRACT(AnulacionDatos,'$.RelLabFchIniActividades')) A_RelLabFchIniActividades,
JSON_UNQUOTE(JSON_EXTRACT(AnulacionDatos,'$.CauBajCod')) A_CauBajCod,
JSON_UNQUOTE((JSON_EXTRACT(AnulacionDatos,'$.RelLabCeseUsrAlta'))) A_RelLabCeseUsrAlta,
JSON_UNQUOTE(JSON_EXTRACT(AnulacionDatos,'$.RelLabCeseFchAlta')) A_RelLabCeseFchAlta,
AnulacionDatos,AnulacionFchAlta 
FROM Personal.ANULACIONES
where
 JSON_VALID(ANULACIONDATOS)=1 
and AnulacionTipoNombre = 'DESIGNACION'
-- RANGO DE PERIODO DE  BANDEJA ************** ----------------------------------------
and (AnulacionFchAlta >= @desdeBandeja and AnulacionFchAlta < @hastaBandeja)
) B

join
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,rellabfchfinprevista,
 PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, DenomCargoSiapId,NivelCargo
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoCesId = DENOMINACIONES_CARGOS.DenomCargoId
where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and P.denomcargoid in(3000,3100,858,859,420,410,411,12356,12355,12354,12353,12352,12360,12361,100022)
and (V.FuncionId = PuestoFuncionId or V.FuncionId = 100006)
and (RL.RelLabFchIniActividades >= '2019-03-01') 

 and (
     (DENOMINACIONES_CARGOS.DenomCargoId in(3000,3100) and(ifnull(RelLabDesignCatLiceo,0) = 0 or DCSC.Categoria = RelLabDesignCatLiceo ))
     or
     (DENOMINACIONES_CARGOS.DenomCargoId not in(3000,3100) and DCSC.Categoria is null)
 )

group by rellabid

) A 

on cast(A.rellabid as char(60)) = B.AnulacionValorPkTabla
)

UNION

-- ********************************** RESERVAS -------------------------------------------------------------
--  ********************************** ------------------------------------------------------------------

(SELECT null DesigId,
1 EmpCod,'DO' PerDocTpo,'UY' PerDocPaisCod,CONVERT(Perdocid,char(24)) PerDocNum,if(Rellabdesigncaracter='S', null,A.Correlativo) CarNum,A.rellabid RelLabId,
DenomCargoSiapId CarCod,Null CarNumVer,NivelCargo CarNiv,'H' EscCod,
if ((if(Rellabdesigncaracter='S', null,A.Correlativo)) is not null,2,5) CatCod,
IniActividades DesFchIng,RelLabCeseFchReal DesFchEgr,A.Personalperid FunCod,
2 DesActLabNum,null SegSalCod,'1.2' LiqUniCod,12 VinFunCod,Caubajcod CauBajCod,21 ComEspCod,Null TipoRemCod,DEP_DBC DesInsCod,IniActividades DesInsFchDesde,RelLabCeseFchReal DesInsFchHasta,
CargaHorariaCantHoras CarRegHor,
CargaHorariaCantHoras DesRegHor,
IniActividades DesRegHorFchDesde,RelLabCeseFchReal DeRegHorFchHasta,Null DesCarId,
(case Rellabdesigncaracter when 'E' then 1 when 'I' then 2 else 3 end) DesCarCod,
Null ForAccCarCod,Null DesGraEsp,Null DesFchFinCon,'' DesObs,null DesFchIniCon, 
(case SuplCausId 
when 6 then  2
when 16 then 4
when 17 then 3
when 39 then 5
when 40 then 7
when 41 then 6
when 43 then 9
end
)SitFunId,
-- --  CODIGO NUEVO 2019-04-03
-- date(SuplfchAlta) SitFunFchDesde,
 date(InicioSupl) SitFunFchDesde,
-- Fin -*-*-*-*-*---------*---------*-

FinSuplencia SitFunFchHasta,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
, Suplencias_FchUltAct BandejaOrden
FROM
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,rellabfchfinprevista,
 PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, DenomCargoSiapId,NivelCargo
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoCesId = DENOMINACIONES_CARGOS.DenomCargoId
where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100

--  CODIGO NUEVO 2019-04-03
 and (V.FuncionId = PuestoFuncionId) 
 and CargaHorariaCantHoras > 0
-- Fin -*-*-*-*-*---------*---------*-

and P.denomcargoid in(3000,3100,858,859,420,410,411,12356,12355,12354,12353,12352,12360,12361,100022)
-- and (V.FuncionId = PuestoFuncionId or V.FuncionId = 100006) SOLO CARGOS SE RESERVAN 

 and (
     (DENOMINACIONES_CARGOS.DenomCargoId in(3000,3100) and(ifnull(RelLabDesignCatLiceo,0) = 0 or DCSC.Categoria = RelLabDesignCatLiceo ))
     or
     (DENOMINACIONES_CARGOS.DenomCargoId not in(3000,3100) and DCSC.Categoria is null)
 )

) A 
-- Se comenta para que no traiga las 8 hrs de director y demas cargos, como pasajes a suplencias por reserva etc.
/* 
left join 
(
select RelLabId,Sum(FuncRelLabCantHrs) horastotales2, FA.FuncAsignadaFchDesde desde, FA.FuncAsignadaFchHasta hasta
from Personal.FUNCIONES_RELACION_LABORAL FRL
join Personal.FUNCIONES_ASIGNADAS FA using (funcasignadaid)
where FuncAsignadaAnulada=0
group by FRL.RelLabId,desde,hasta
) C
on A.rellabid = C.rellabid
and A.IniActividades = desde
and (hasta is null or A.IniActividades <= hasta)
*/
join
(
select S.*, RLS.RelLabCeseFchReal FinSuplencia, RLS.PersonalPerId,
--  CODIGO NUEVO 2019-04-03
greatest(S.SuplFchAlta, RLT.RelLabFchIniActividades) InicioSupl
-- Fin -*-*-*-*-*---------*---------*-
from Personal.SUPLENCIAS S
join Personal.RELACIONES_LABORALES RLS on RLS.RelLabId = S.Suplrellabid
join Personal.RELACIONES_LABORALES RLT on RLT.RelLabId = S.RelLabId
where S.SuplCausId in(6,16,17,39,40,41,43)

and RLS.RelLabCeseFchReal >='2019-03-01'
and (S.Suplencias_FchUltAct >= @desdeBandeja and S.Suplencias_FchUltAct < @hastaBandeja)

)B
on A.Rellabid = B.RelLabId
--  CODIGO NUEVO 2019-04-03
-- group by RelLabId -- se precisan todas las lineas de pasajes a suplencias
-- Se necesita agrupar para que no genere 2 registros para los casos de 40 + 8
order by DenomCargoId, SitFunFchDesde, SitFunFchHasta
)

)U

order by BandejaOrden;
