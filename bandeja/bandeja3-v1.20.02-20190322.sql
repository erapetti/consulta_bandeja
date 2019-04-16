start transaction;

use siap_ces_tray;

/*
Select *
FROM Personal.idesignaciones I
Where 
I.SitFunId is NULL and I.CauBajCod<>99 and 
I.DesigId not in (
	SELECT I.DesigId, I.DesFchIng as IFecha, D.DesFchIng as DFecha
	FROM 
	Personal.idesignaciones I,
	siap_ces.DESIGNACIONES D
	where 
	I.SitFunId is NULL and I.CauBajCod<>99
	and I.FunCod=D.FunCod 
	and I.DesFchIng = D.DesFchIng
);
*/

-- --------------------------------------
-- Arreglo fecha de inicio de las designaciones 2018
/*
Update
	idesignaciones I,
	siap_ces.DESIGNACIONES D
Set I.DesFchIng=D.DesFchIng
Where 
I.SitFunId is NULL and I.CauBajCod<>99
and I.FunCod=D.FunCod and
I.DesigId not in (
	SELECT I.DesigId -- , I.DesFchIng as IFecha, D.DesFchIng as DFecha
	FROM 
	Personal.idesignaciones I,
	siap_ces.DESIGNACIONES D
	where 
	I.SitFunId is NULL and I.CauBajCod<>99
	and I.FunCod=D.FunCod 
	and I.DesFchIng = D.DesFchIng
);
*/

/*
update idesignaciones I,     siap_ces.DESIGNACIONES D     join siap_ces.CARGOSNUMEROS C using(EmpCod,carnum,CarNumVer),     siap_ces.designacionescaracteres DCT
set I.DesFchIng=D.DesFchIng, I.DesInsFchDesde=D.DesFchIng, I.DesRegHorFchDesde=D.DesFchIng
where I.SitFunId is NULL
  and I.CauBajCod<>99
  and I.FunCod=D.FunCod
  and  C.carcod = I.carcod
  and D.DesCarId = DCT.DesCarId
  and I.DesCarCod = DCT.DesCarCod
  and (D.DesFchIng < '2019-03-01')
  and (I.DesFchIng < '2019-03-01')
  and (D.DesFchEgr = '1000-01-01' or D.DesFchEgr >= '2019-02-28')
  and (I.DesFchIng<>D.DesFchIng or I.DesInsFchDesde<>D.DesFchIng or I.DesRegHorFchDesde<>D.DesFchIng)
  and I.DesFchEgr ='2019-02-28'
  and I.EmpCod=D.EmpCod
;
*/


-- --------------------------------------
-- Arreglo fechas NULL

update idesignaciones
set DesFchIng=if(DesFchIng is null or DesFchIng='0000-00-00','1000-01-01',DesFchIng),
    DesFchEgr=if((DesFchEgr is null or DesFchEgr='0000-00-00') and CauBajCod<>99,'1000-01-01',DesFchEgr),
    DesInsFchDesde=if(DesInsFchDesde is null or DesInsFchDesde='0000-00-00','1000-01-01',DesInsFchDesde),
    DesInsFchHasta=if(DesInsFchHasta is null or DesInsFchHasta='0000-00-00','1000-01-01',DesInsFchHasta),
    DesRegHorFchDesde=if(DesRegHorFchDesde is null or DesRegHorFchDesde='0000-00-00','1000-01-01',DesRegHorFchDesde),
    DeRegHorFchHasta=if(DeRegHorFchHasta is null or DeRegHorFchHasta='0000-00-00','1000-01-01',DeRegHorFchHasta),
    DesFchFinCon=if(DesFchFinCon is null or DesFchFinCon='0000-00-00','1000-01-01',DesFchFinCon),
    DesFchIniCon=if(DesFchIniCon is null or DesFchIniCon='0000-00-00','1000-01-01',DesFchIniCon),
    SitFunFchDesde=if(SitFunFchDesde is null or SitFunFchDesde='0000-00-00','1000-01-01',SitFunFchDesde),
    SitFunFchHasta=if(SitFunFchHasta is null or SitFunFchHasta='0000-00-00','1000-01-01',SitFunFchHasta),
    DesFchCarga=if(DesFchCarga is null or DesFchCarga='0000-00-00','1000-01-01',DesFchCarga)

where resultado='PE' and (

DesFchIng is null or DesFchEgr is null or DesInsFchDesde is null or DesInsFchHasta is null or DesRegHorFchDesde is null or DeRegHorFchHasta is null or DesFchFinCon is null or DesFchIniCon is null or SitFunFchDesde is null or SitFunFchHasta is null or DesFchCarga is null
) or (
DesFchIng='0000-00-00' or DesFchEgr='0000-00-00' or DesInsFchDesde='0000-00-00' or DesInsFchHasta='0000-00-00' or DesRegHorFchDesde='0000-00-00' or DeRegHorFchHasta='0000-00-00' or DesFchFinCon='0000-00-00' or DesFchIniCon='0000-00-00' or SitFunFchDesde='0000-00-00' or SitFunFchHasta='0000-00-00' or DesFchCarga='0000-00-00'
 );


-- --------------------------------------
-- Bajas por traslado

update idesignaciones
set CauBajCod=34
where resultado='PE'
  and CauBajCod=150;


-- --------------------------------------
-- Cargas horarias cero no van por la bandeja

delete from idesignaciones where CarRegHor=0;


commit;
