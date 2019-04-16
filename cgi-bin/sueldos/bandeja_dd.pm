#!/usr/bin/perl
#
# bandeja_dd.pm
#
#	Bandeja de docencia directa


use strict;


package bandeja_dd;

sub resumen($) {
	my ($dbh) = @_;

	my $sth = portal3::dbGet($dbh, "siap_ces_tray.ihorasclase",
			["DesFchCarga","count(distinct perdocnum) Personas","count(*) Registros"],
			"HorClaEmpCod=1 and DesFchProc is null and DesFchCarga is not null",
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

SELECT HC3.perdocnum,
       HC3.HorClaFchPos FchPos,
       HC3.HorClaFchCese FchCese,
       InsDsc Dependencia,
       group_concat(HC3.RelLabId separator ', '),
       AsiNom Asign,
       HC3.HorClaCic Ciclo,
       sum(HC3.horclahor) Horas,
       HC3.DesFchCarga,
       ifnull(HC3.DesFchProc,'') DesFchProc,
       concat(if(HC3.HorClaBajLog,'Baja lógica ',''),if(HC3.DesFchProc is null and HC3.mensaje='','Pendiente',left(replace(replace(replace(convert(HC3.mensaje using 'utf8'),char(34),''),char(39),''),'ERROR: ',''),60))) mensaje,
       HC3.NroLote
FROM (select HC1.perdocnum,
             HC1.DesFchCarga,
             HC1.DesFchProc,
             HC1.nrolote
      from ihorasclase HC1
      left join ihorasclase HC2
             on HC2.perdocnum=HC1.perdocnum
            and (HC1.DesFchCarga<HC2.DesFchCarga
                 or
                 HC1.DesFchCarga=HC2.DesFchCarga
                 and (isnull(HC2.DesFchProc) and (HC1.DesFchProc is not null)
                      or
                      HC1.NroLote<HC2.NroLote
                 )
            )
      where HC1.perdocnum='".$cedula."'
        and HC1.DesFchCarga is not null
        and HC2.perdocnum is null
      limit 1) ULT
JOIN ihorasclase HC3
  ON HC3.perdocnum=ULT.perdocnum
 AND ((ULT.DesFchProc IS NOT NULL AND HC3.DesFchProc=ULT.DesFchProc AND (HC3.nrolote IS NULL OR HC3.nrolote=ULT.nrolote))
      OR
      (ULT.DesFchProc IS NULL AND HC3.DesFchProc IS NULL AND HC3.DesFchCarga=ULT.DesFchCarga)
     )
JOIN siap_ces.institucionales
  ON HorClaInsCod=InsCod
LEFT JOIN siap_ces.asignaturas
  ON AsiCod=HC3.HorClaAsiCod
WHERE NOT(HC3.HorClaBajLog=1 AND HC3.HorClaCauBajCod=99)
  AND HC3.DesFchCarga>='2019-03-01'
GROUP BY 1,2,3,4,6,7,9,10,11,12
ORDER BY 2,4,6,7,3

	");
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","RelLab","Asignatura","Ciclo","Horas","FchCarga","FchProc","Mensaje","Lote"], data=>$rows};
}

sub errores($) {
	my ($dbh) = @_;

	my $sth = $dbh->prepare("

select HC1.DesFchCarga `Fecha de carga`,HC1.perdocnum Cédula,errores
from (
      select NroLote,DesFchCarga,perdocnum,count(*) errores
      from ihorasclase
      where not(HorClaBajLog=1 and HorClaCauBajCod=99)
      and DesFchCarga >= '2019-03-01'
      and resultado not in ('OK','PE')
      group by 1,2,3
) HC1
left join ihorasclase HC2
  on HC1.perdocnum=HC2.perdocnum
 and (HC1.DesFchCarga<HC2.DesFchCarga
      or (HC1.DesFchCarga=HC2.DesFchCarga and (HC2.nrolote is null or HC2.nrolote is not null and HC1.nrolote<HC2.nrolote))
     )
where HC2.perdocnum is null
order by HC1.DesFchCarga desc,2

	");
	$sth->execute;

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Fecha Carga","Cédula","Errores"], data=>$rows};
}

1;
