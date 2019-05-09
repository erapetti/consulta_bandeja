#!/usr/bin/perl
#
# bandeja_di.pm
#
#	Bandeja de docencia indirecta


use strict;


package bandeja_di;

sub resumen($) {
	my ($dbh) = @_;

	my $sth = portal3::dbGet($dbh, "siap_ces_tray.idesignaciones",
			["DesFchCarga","count(distinct perdocnum) Personas","count(*) Registros"],
			"EmpCod=1 and DesFchProc is null and DesFchCarga is not null",
			"group by 1 order by 1"
	               );

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Fecha Carga","Personas","Registros"], data=>$rows};
}

sub buscar($$) {
	my ($dbh, $cedula) = @_;

	my $sth = $dbh->prepare("

SELECT perdocnum,
       DesFchIng FchPos,
       DesFchEgr FchCese,
       InsDsc Dependencia,
       RelLabId RelLabId,
       CarDsc Cargo,
       CarNum CarNum,
       sum(CarRegHor) Horas,
       group_concat(if(SitFunId>1,concat(SitFunFchDesde,' a ',SitFunFchHasta,' '),'')) Reservas,
       concat(if(CauBajCod=99,'Baja lógica ',''),if(DesFchProc is null and mensaje='','Pendiente',concat(DesFchProc,' ',left(replace(replace(replace(convert(mensaje using 'utf8'),char(34),''),char(39),''),'ERROR: ',''),60)))) mensaje,
       NroLote
FROM (select ID1.*
      from idesignaciones ID1
      left join idesignaciones ID2
        on ID2.RelLabId=ID1.RelLabId
       and ID2.DesFchIng=ID1.DesFchIng
       and (isnull(ID1.SitFunId) or isnull(ID2.SitFunId) or (ifnull(ID1.SitFunId,0) = ifnull(ID2.SitFunId,0)))
       and (ifnull(ID1.SitFunId,0) in (0,1) or ID1.SitFunFchDesde=ID2.SitFunFchDesde)
       and ID2.DesigId>ID1.DesigId
       and ifnull(ID2.Resultado,'') in ('','OK','ERROR','PE')
       where ID2.RelLabId is null
) ULT
JOIN siap_ces.institucionales
  ON DesInsCod=InsCod
LEFT JOIN siap_ces.cargos
  USING (CarCod)
WHERE DesFchCarga>='2019-03-01'
  AND perdocnum='".$cedula."'
  AND ifnull(Resultado,'') in ('','OK','ERROR','PE')
GROUP BY 1,2,3,4,5,6,7,10,11,CauBajCod
ORDER BY 2,3,4,5,6,7,DesigId

	");
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","RelLab","Cargo","CarNum","Horas","Reservas","Mensaje","Lote"], data=>$rows};
}

sub errores($) {
	my ($dbh) = @_;

	my $sth = $dbh->prepare("

-- select max(DesFchCarga) `Fecha de carga`,ULT.perdocnum,concat(PerPriApe,' ',', ',PerPriNom,' ',ifnull(PerSegNom,'')) nombre,count(*) errores
select max(DesFchCarga) `Fecha de carga`,
       ULT.perdocnum,
       replace(ifnull(concat(PerPriApe,' ',ifnull(PerSegApe,''),', ',PerPriNom,' ',ifnull(PerSegNom,'')),''),char(39),char(44)) nombre,
       count(*) errores
-- select max(DesFchCarga) `Fecha de carga`,ULT.perdocnum,'',count(*) errores
FROM (select ID1.*
      from idesignaciones ID1
      left join idesignaciones ID2
        on ID2.RelLabId=ID1.RelLabId
       and ID2.DesFchIng=ID1.DesFchIng
       and ifnull(ID1.SitFunId,0)=ifnull(ID2.SitFunId,0)
       and (ifnull(ID1.SitFunId,0) in (0,1) or ID1.SitFunFchDesde=ID2.SitFunFchDesde)
       and ID2.DesigId>ID1.DesigId
       and ifnull(ID2.Resultado,'') in ('','OK','ERROR','PE')
       where ID2.RelLabId is null
) ULT
left join siap_ces.personas p using (perdocnum)
WHERE resultado='ERROR'
  AND DesFchCarga >= '2019-03-01'
GROUP BY 2
ORDER BY 1 DESC,2

	");
	$sth->execute;

	(defined($sth) && !$DBI::errstr) or return $DBI::errstr;
	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Fecha Carga","Cédula","Nombre","Errores"], data=>$rows};
}

1;
