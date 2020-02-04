
set @desde=concat(year(curdate())-if(month(curdate())<=2,1,0),'-03-01');

-- ----------------------------------------------------------------------------------------------------------------------------------
-- -- Todas las designaicones Interinas y Suplentes sin fecha de registro de cese, 
-- -- se actualizara con la fecha de registro del alta de la designacion


select 'FA con diferente actualización del cese que la RL' mensaje,ifnull(sum(RelLabCeseFchAlta <> RelLabDesignFchAlta),0) cant
from RELACIONES_LABORALES RL
join PUESTOS P on P.Puestoid = RL.PuestoId
join (select DenomCargoCesId denomcargoid from DENOMINACIONES_CARGOS_SIAP_CES group by 1) DC USING (denomcargoid)
where (RelLabDesignCaracter = 'I' or RelLabDesignCaracter = 'S')
and RelLabCeseFchAlta is null 
and RelLabFchIniActividades > '2018-03-01'
and migradd_lote is null
;

-- ----------------------------------------------------------------------------------------------------------------------------------
-- las anulaciones de los ceses se grabe en la tabla RELACIONES_LABORALES en el campo RelLabCeseFchAlta, 
-- la fecha en la que se realiza la anulación y por tanto se corrige la tabla (RELACIONES_LABORALES) 
-- con datos como causal de baja y fecha real de cese.

select 'RL con menor fecha actualización del cese que la anulación' mensaje,ifnull(sum(ifnull(RelLabCeseFchAlta,'1000-01-01') < AnulacionFchAlta),0) cant
from RELACIONES_LABORALES
join (select AnulacionValorPkTabla,max(AnulacionFchAlta)AnulacionFchAlta
      from ANULACIONES
      where AnulacionTipoNombre = 'CESE_DESIGNACION'
        and JSON_VALID(ANULACIONDATOS)=1
      group by 1
)A on cast(Rellabid as char(60))= AnulacionValorPkTabla
where (RelLabCeseFchAlta is null
     or DATE(RelLabCeseFchAlta) < DATE(AnulacionFchAlta)
)
  and RelLabVacanteAnioLectivo>=2019
  and migradd_lote is null
;

-- ----------------------------------------------------------------------------------------------------------------------------------
-- -- Verifico que los institucionales están creados

select 'Institucionales que faltan en siap_ces_tray.tabla_institucional' mensaje,count(*) cant
from (
   select v.dependid
   from v_funciones_del_personal v
   join (select DenomCargoCesId denomcargoid from DENOMINACIONES_CARGOS_SIAP_CES group by 1) DC USING (denomcargoid)
   where estado='A'
   group by 1
) D
left join siap_ces_tray.tabla_institucional ti
  on dep_as400=dependid
where ti.dep_as400 is null;

-- ----------------------------------------------------------------------------------------------------------------------------------
-- -- No debemos tener designaciones interinas o efectivas sin número de correlativo

/* comento hasta arreglar los datos
select 'Efectivos o interinos sin correlativo' mensaje,count(*) cant
from v_funciones_del_personal
join PUESTOS USING (PUESTOID,denomcargoid)
join (select DenomCargoCesId denomcargoid from DENOMINACIONES_CARGOS_SIAP_CES group by 1) DC USING (denomcargoid)
where estado='A'
  and rellabdesigNcaracter in ('E','I')
  and correlativo is null;
*/

-- ----------------------------------------------------------------------------------------------------------------------------------
-- -- No debemos tener suplencias del año pasado

select 'Suplencias del año anterior' mensaje,count(*) cant
from v_funciones_del_personal v
join (select DenomCargoCesId denomcargoid from DENOMINACIONES_CARGOS_SIAP_CES group by 1) DC USING (denomcargoid)
join SUPLENCIAS S using (RelLabId)
join RELACIONES_LABORALES RLS ON RLS.RelLabId=SuplRelLabId
where estado='A'
  and greatest(RLS.RelLabFchIniActividades,adddate(ifnull(date(SuplFchAlta),'1000-01-01'),1)) < @desde
  and RLS.RelLabAnulada=0
  and (ifnull(RLS.RelLabCeseFchReal,'1000-01-01') = '1000-01-01' or RLS.RelLabCeseFchReal > @desde)
  and migradd_lote is null
;

-- ----------------------------------------------------------------------------------------------------------------------------------
-- -- Los registros en la tabla de conversión que no son de directores o subdirectores no tienen que tener categoría

select 'Mapeos de no directores con categoría' mensaje,count(*) cant
from DENOMINACIONES_CARGOS_SIAP_CES DCSC
where (DenomCargoCesId not in(3000,3100) and DCSC.Categoria is not null);

