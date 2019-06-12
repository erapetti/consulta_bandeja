use strict;

package viaticos;


sub buscar($$) {
	my ($dbh,$cedula) = @_;

	my $SQL= "

SELECT PerDocNum Cédula,
       ViatAnio,
       ViatMes,
       ViatCant,
       ViatFchCarga,
       ViatFchProc,
       Mensaje
FROM (
  select max(ViatMes) mes,
         max(ViatAnio) anio
  from siap_ces_tray.iviaticos
  where PerDocNum='$cedula'
) FC
join siap_ces_tray.iviaticos M1
  on ViatMes=mes
 and ViatAnio=anio
WHERE PerDocNum='$cedula'
  AND ViatCant>0;

	";
        my $sth = $dbh->prepare($SQL);
        $sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Año","Mes","Horas","FchCarga","FchProc","Mensaje"], data=>$rows};
}

sub resumen($) {
	my ($dbh) = @_;

	my $sth = portal3::dbGet($dbh, "siap_ces_tray.iviaticos",
			["ViatFchCarga","count(distinct perdocnum) Personas","count(*) Registros"],
			"ViatFchProc is null and ViatFchCarga is not null",
			"group by 1 order by 1"
	               );

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Fecha Carga","Personas","Registros"], data=>$rows};
}

sub errores($) {
	my ($dbh) = @_;

	my $sth = $dbh->prepare("

select max(ViatFchCarga) `Fecha de carga`,
       ULT.perdocnum,
       replace(ifnull(concat(PerPriApe,' ',ifnull(PerSegApe,''),', ',PerPriNom,' ',ifnull(PerSegNom,'')),''),char(39),char(44)) nombre,
       count(*) errores
FROM (select VI1.*
      from iviaticos VI1
      left join iviaticos VI2
        on VI2.perdocnum=VI1.perdocnum
       and VI2.ViatAnio=VI1.ViatAnio
       and VI2.ViatMes=VI1.ViatMes
       and VI2.ViatId>VI1.ViatId
       and ifnull(VI2.Resultado,'') in ('','OK','ERROR','PE')
       where VI2.ViatId is null
) ULT
left join siap_ces.personas p using (perdocnum)
WHERE resultado='ERROR'
  AND ViatFchCarga >= '2019-03-01'
GROUP BY 2
ORDER BY 1 DESC,2

	");
	$sth->execute;

	(defined($sth) && !$DBI::errstr) or return $DBI::errstr;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Fecha Carga","Cédula","Nombre","Errores"], data=>$rows};
}

1;
