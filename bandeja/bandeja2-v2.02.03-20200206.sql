-- ----------------------------------------------------------------------------------------------------------------------------------
-- -- Todas las designaicones Interinas y Suplentes sin fecha de registro de cese, 
-- -- se actualizara con la fecha de registro del alta de la designacion

--  ********************************** ORDEN DE BANDEJA -------------------------------------------------------------
--  ********************************** -------------------------------------------------------------



use siap_ces_tray;

-- Variables para el RANGO DE PERIODO DE  BANDEJA, en producción se reemplazan por otros valores
set @desdeBandeja='2019-08-07 10:47:03';
set @hastaBandeja ='2019-08-19 18:55:00';


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
CargaHorariaPuesto CarRegHor,CargaHorariaPuesto DesRegHor,IniActividades DesRegHorFchDesde,RelLabCeseFchReal DeRegHorFchHasta,Null DesCarId,
(case Rellabdesigncaracter when 'E' then 1 when 'I' then 2 else 3 end) DesCarCod,
Null ForAccCarCod,Null DesGraEsp,Null DesFchFinCon,'' DesObs,null DesFchIniCon, 
1 SitFunId,IniActividades SitFunFchDesde,RelLabCeseFchReal SitFunFchHasta,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
,FuncAsignadaFchAlta BandejaOrden
FROM
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,rellabfchfinprevista,
 PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, 
-- CODIGO NUEVO 2019-07-02
PA.DenomCargoSiapId,
NivelCargo
,FuncAsignadaFchAlta
,CH.CargaHorariaCantHoras CargaHorariaPuesto
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
-- CODIGO NUEVO 2019-07-02
join Personal.PADRON PA on P.Correlativo = PA.Correlativo

join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
-- CODIGO NUEVO 2019-07-02
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoSiapId = PA.DenomCargoSiapId
join Personal.CARGAS_HORARIAS CH on P.PuestoCargaHorariaId = CH.CargaHorariaId

where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and (V.FuncionId = PuestoFuncionId)
and (RL.RelLabFchIniActividades >= '2019-03-01') 
-- RANGO DE PERIODO DE  BANDEJA ************** ----------------------------------------
and (FA.FuncAsignadaFchAlta >= @desdeBandeja and FA.FuncAsignadaFchAlta < @hastaBandeja)
and (FA.FuncAsignadaFchDesde >= '2019-03-01')
) A 

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
8,IniActividades,RelLabCeseFchReal,
-- null,null,null,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
,RelLabCeseFchAlta BandejaOrden
from
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,
rellabfchfinprevista, RelLabCeseFchAlta,P.Correlativo,DEP_DBC,
-- CODIGO NUEVO 2019-07-02
PA.DenomCargoSiapId,
NivelCargo
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
-- CODIGO NUEVO 2019-07-02
join Personal.PADRON PA on P.Correlativo = PA.Correlativo
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
-- CODIGO NUEVO 2019-07-02
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoSiapId = PA.DenomCargoSiapId
where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and V.FuncionId = PuestoFuncionId
-- Fecha de Registro del CESE de la Relacion Laboral -* DESDE -----------------------
-- and RL.RelLabCeseFchAlta > '2018-08-09' 

-- RANGO DE PERIODO DE  BANDEJA ************** ----------------------------------------
-- En la negacion de las altas (abajo) para el periodo de la bandeja, 
-- no se toma en cuenta la fecha de registro de funcion asignada, se usa la de la Relacion Laboral
-- (RelLabDesignFchAlta)
-- como si se hace en las Altas (donde se toma en cuenta el + 8 para Directores etc.)
-- Resumen no se toma en cuenta el + 8 de Fucnion asignda para Directores etc.

and (RL.RelLabCeseFchAlta  >= @desdeBandeja and RL.RelLabCeseFchAlta < @hastaBandeja)
and not (RL.RelLabDesignFchAlta >= @desdeBandeja and RL.RelLabDesignFchAlta < @hastaBandeja)

 and
 ( 
  (ifnull(RL.RelLabCeseFchReal,'1000-01-01')='1000-01-01' or RL.RelLabCeseFchReal >= '2019-03-01')
 and 
(-- RL.RelLabFchIniActividades >= '2019-03-01' and 
RL.RelLabCeseFchAlta <> RL.RelLabDesignFchAlta)
 )
 and (ifnull(FuncAsignadaFchHasta,'1000-01-01')='1000-01-01' or FuncAsignadaFchHasta >= '2019-03-01')
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
 PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, 
-- CODIGO NUEVO 2019-07-02
PA.DenomCargoSiapId,
NivelCargo
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
-- CODIGO NUEVO 2019-07-02
join Personal.PADRON PA on P.Correlativo = PA.Correlativo
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
-- CODIGO NUEVO 2019-07-02
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoSiapId = PA.DenomCargoSiapId

where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and (V.FuncionId = PuestoFuncionId or V.FuncionId = 100006)
and (RL.RelLabFchIniActividades >= '2019-03-01') 

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
 PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, PA.DenomCargoSiapId,NivelCargo
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
-- CODIGO NUEVO 2019-07-02
join Personal.PADRON PA on P.Correlativo = PA.Correlativo
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
-- CODIGO NUEVO 2019-07-02
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoSiapId = PA.DenomCargoSiapId
where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100

--  CODIGO NUEVO 2019-04-03
 and (V.FuncionId = PuestoFuncionId) 
 and CargaHorariaCantHoras > 0
-- Fin -*-*-*-*-*---------*---------*-

-- and (RL.Rellabdesigncaracter = 'E' or RL.Rellabdesigncaracter = 'I')
-- and Correlativo is null
) A 

join
(
select S.*, RLS.RelLabCeseFchReal FinSuplencia, RLS.PersonalPerId,
--  CODIGO NUEVO 2019-05-31
greatest(adddate(ifnull(date(S.SuplFchAlta),'1000-01-01'),1), RLT.RelLabFchIniActividades) InicioSupl
-- Fin -*-*-*-*-*---------*---------*-
from Personal.SUPLENCIAS S
join Personal.RELACIONES_LABORALES RLS on RLS.RelLabId = S.Suplrellabid
join Personal.RELACIONES_LABORALES RLT on RLT.RelLabId = S.RelLabId
where S.SuplCausId in(6,16,17,39,40,41,43)

and RLS.RelLabCeseFchReal >='2019-03-01'
and (S.Suplencias_FchUltAct >= @desdeBandeja and S.Suplencias_FchUltAct < @hastaBandeja)

)B
on A.Rellabid = B.RelLabId
-- controlar fecha de Funciones Asignadas 2019-04-11
and (ifnull(A.Funcasignadafchhasta,'1000-01-01')='1000-01-01' or A.Funcasignadafchhasta >= B.SuplFchAlta)
and (A.FuncAsignadaFchDesde < B.FinSuplencia)
--  CODIGO NUEVO 2019-04-03
-- group by RelLabId -- se precisan todas las lineas de pasajes a suplencias
order by DenomCargoId, SitFunFchDesde, SitFunFchHasta
)


UNION

-- ********************************** BAJAS DE RESERVAS -------------------------------------------------
--  ********************************** ------------------------------------------------------------------

(SELECT null DesigId,
1 EmpCod,'DO' PerDocTpo,'UY' PerDocPaisCod,CONVERT(Perdocid,char(24)) PerDocNum,if(Rellabdesigncaracter='S', null,D.Correlativo) CarNum,D.rellabid RelLabId,
DenomCargoSiapId CarCod,Null CarNumVer,NivelCargo CarNiv,'H' EscCod,
if ((if(Rellabdesigncaracter='S', null,D.Correlativo)) is not null,2,5) CatCod,
IniActividades DesFchIng,RelLabCeseFchReal DesFchEgr,D.Personalperid FunCod,
2 DesActLabNum,null SegSalCod,'1.2' LiqUniCod,12 VinFunCod,Caubajcod CauBajCod,21 ComEspCod,Null TipoRemCod,DEP_DBC DesInsCod,IniActividades DesInsFchDesde,RelLabCeseFchReal DesInsFchHasta,
CargaHorariaCantHoras CarRegHor,
CargaHorariaCantHoras DesRegHor,
IniActividades DesRegHorFchDesde,RelLabCeseFchReal DeRegHorFchHasta,Null DesCarId,
(case Rellabdesigncaracter when 'E' then 1 when 'I' then 2 else 3 end) DesCarCod,
Null ForAccCarCod,Null DesGraEsp,Null DesFchFinCon,'' DesObs,null DesFchIniCon, 
(case SuplCausId 
when 6 then  102
when 16 then 104
when 17 then 103
when 39 then 105
when 40 then 107
when 41 then 106
when 43 then 109
end
)SitFunId,
 date(InicioSupl) SitFunFchDesde,
A.A_RelLabCeseFchReal SitFunFchHasta,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
,AnulacionFchAlta BandejaOrden
FROM
-- ANULACION
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
-- RANGO DE PERIODO DE  BANDEJA
and (AnulacionFchAlta >= @desdeBandeja and AnulacionFchAlta < @hastaBandeja)
) A
-- RESERVA
join
(
select S.* , RLS.RelLabCeseFchReal FinSuplencia, RLS.PersonalPerId,
greatest(adddate(ifnull(date(S.SuplFchAlta),'1000-01-01'),1), RLT.RelLabFchIniActividades) InicioSupl
from Personal.SUPLENCIAS S
join Personal.RELACIONES_LABORALES RLT on RLT.RelLabId = S.RelLabId
-- por datos de la reserva para mostrar si es necesario
join Personal.RELACIONES_LABORALES RLS on RLS.RelLabId = S.Suplrellabid
where S.SuplCausId in(6,16,17,39,40,41,43)
)B
on cast(B.Suplrellabid as char(60)) = A.AnulacionValorPkTabla
-- DATOS RL que anulara RESERVA
join
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,rellabfchfinprevista,
 PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, PA.DenomCargoSiapId,NivelCargo
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
join Personal.PADRON PA on P.Correlativo = PA.Correlativo
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoSiapId = PA.DenomCargoSiapId
where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and (V.FuncionId = PuestoFuncionId) 
and CargaHorariaCantHoras > 0
) D 

on D.rellabid = B.RelLabId
)

UNION

-- ********************************** ALTAS Extension Horaria-- (+ 8) -----------------------------------------
--  ********************************** ------------------------------------------------------------------
(SELECT null DesigId,
1 EmpCod,'DO' PerDocTpo,'UY' PerDocPaisCod,CONVERT(Perdocid,char(24)) PerDocNum,if(Rellabdesigncaracter='S', null,A.Correlativo) CarNum,A.rellabid RelLabId,
DenomCargoSiapId CarCod,Null CarNumVer,NivelCargo CarNiv,'H' EscCod,
if ((if(Rellabdesigncaracter='S', null,A.Correlativo)) is not null,2,5) CatCod,
IniActividades DesFchIng,RelLabCeseFchReal DesFchEgr,A.Personalperid FunCod,
2 DesActLabNum,null SegSalCod,'1.2' LiqUniCod,12 VinFunCod,RLCaubajcod CauBajCod,21 ComEspCod,Null TipoRemCod,DEP_DBC DesInsCod,IniActividades DesInsFchDesde,RelLabCeseFchReal DesInsFchHasta,
CargaHorariaPuesto CarRegHor,
-- Nuevo
(CargaHorariaPuesto + CargaHorariaCantHoras) DesRegHor,
FuncAsignadaFchDesde DesRegHorFchDesde,FuncAsignadaFchHasta DeRegHorFchHasta,Null DesCarId,
(case Rellabdesigncaracter when 'E' then 1 when 'I' then 2 else 3 end) DesCarCod,
Null ForAccCarCod,Null DesGraEsp,Null DesFchFinCon,'' DesObs,null DesFchIniCon, 
1 SitFunId,IniActividades SitFunFchDesde,RelLabCeseFchReal SitFunFchHasta,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
,FuncAsignadaFchAlta BandejaOrden
FROM
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,rellabfchfinprevista,
PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, 
PA.DenomCargoSiapId,NivelCargo,FuncAsignadaFchAlta, 
CH.CargaHorariaCantHoras CargaHorariaPuesto, RL.CauBajCod RLCaubajcod
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
join Personal.PADRON PA on P.Correlativo = PA.Correlativo
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoSiapId = PA.DenomCargoSiapId
join Personal.CARGAS_HORARIAS CH on P.PuestoCargaHorariaId = CH.CargaHorariaId

where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and (V.FuncionId = 100006)
-- and (RL.RelLabFchIniActividades >= '2019-03-01') 
-- RANGO DE PERIODO DE  BANDEJA ************** ----------------------------------------
and (FA.FuncAsignadaFchAlta >= @desdeBandeja and FA.FuncAsignadaFchAlta < @hastaBandeja)
and (FA.FuncAsignadaFchDesde >= '2019-03-01')
)A)


-- ********************************** BAJA Extension Horaria-- (- 8) -----------------------------------------
--  ********************************** ------------------------------------------------------------------
UNION
(SELECT null DesigId,
1 EmpCod,'DO' PerDocTpo,'UY' PerDocPaisCod,CONVERT(Perdocid,char(24)) PerDocNum,if(Rellabdesigncaracter='S', null,A.Correlativo) CarNum,A.rellabid RelLabId,
DenomCargoSiapId CarCod,Null CarNumVer,NivelCargo CarNiv,'H' EscCod,
if ((if(Rellabdesigncaracter='S', null,A.Correlativo)) is not null,2,5) CatCod,
IniActividades DesFchIng,RelLabCeseFchReal DesFchEgr,A.Personalperid FunCod,
2 DesActLabNum,null SegSalCod,'1.2' LiqUniCod,12 VinFunCod,RLCaubajcod CauBajCod,21 ComEspCod,Null TipoRemCod,DEP_DBC DesInsCod,IniActividades DesInsFchDesde,RelLabCeseFchReal DesInsFchHasta,
CargaHorariaPuesto CarRegHor,
-- Nuevo
(CargaHorariaPuesto + CargaHorariaCantHoras) DesRegHor,
FuncAsignadaFchDesde DesRegHorFchDesde,FuncAsignadaFchHasta DeRegHorFchHasta,Null DesCarId,
(case Rellabdesigncaracter when 'E' then 1 when 'I' then 2 else 3 end) DesCarCod,
Null ForAccCarCod,Null DesGraEsp,Null DesFchFinCon,'' DesObs,null DesFchIniCon, 
1 SitFunId,IniActividades SitFunFchDesde,RelLabCeseFchReal SitFunFchHasta,
Null LiqGruCod,null DesFunCod,curdate() DesFchCarga,null DesFchProc,'PE' Resultado,'' Mensaje,null NroLote
,FuncAsignadaCeseFchAlta BandejaOrden
FROM
(
SELECT V.*,RelLabCeseFchReal,RelLabFchIniActividades IniActividades,rellabfchfinprevista,
PuestoFuncionId, RelLabDesignFchAlta , P.Correlativo ,DEP_DBC, 
PA.DenomCargoSiapId,NivelCargo,FuncAsignadaFchAlta, 
CH.CargaHorariaCantHoras CargaHorariaPuesto, FuncAsignadaCeseFchAlta, RL.CauBajCod RLCaubajcod
FROM Personal.v_funciones_del_personal_con_anulaciones as V
join Personal.PUESTOS P on P.Puestoid = V.PuestoId
join Personal.PADRON PA on P.Correlativo = PA.Correlativo
join Personal.DENOMINACIONES_CARGOS on P.denomcargoid = DENOMINACIONES_CARGOS.DenomCargoId
join Personal.RELACIONES_LABORALES RL on RL.RelLabId = V.RelLabId
join Personal.FUNCIONES_ASIGNADAS FA on V.FuncAsignadaId = FA.FuncAsignadaId
join siap_ces_tray.tabla_institucional TI on TI.DEP_AS400 = V.DependId
join Personal.DENOMINACIONES_CARGOS_SIAP_CES DCSC on DCSC.DenomCargoSiapId = PA.DenomCargoSiapId
join Personal.CARGAS_HORARIAS CH on P.PuestoCargaHorariaId = CH.CargaHorariaId

where
DENOMINACIONES_CARGOS.escid = 'H'
and PuestoFuncionId <> 100
and (V.FuncionId = 100006)
-- and (RL.RelLabFchIniActividades >= '2019-03-01') 
-- RANGO DE PERIODO DE  BANDEJA ************** ----------------------------------------
and (FA.FuncAsignadaCeseFchAlta >= @desdeBandeja and FA.FuncAsignadaCeseFchAlta< @hastaBandeja)
and (FA.FuncAsignadaFchDesde >= '2019-03-01') 
and (FA.FuncAsignadaCeseFchAlta <> FA.FuncAsignadaFchAlta)
) A )


)U

order by BandejaOrden;
