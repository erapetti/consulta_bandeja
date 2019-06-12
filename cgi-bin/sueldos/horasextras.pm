use strict;

package horasextras;

sub buscar($$) {
	my ($dbh,$cedula) = @_;

	my $SQL= "

SELECT PerDocNum Cédula,
       HorasAnio,
       HorasMes,
       HorExtrasCant,
       HorasFchCarga,
       HorasFchProc,
       Mensaje
FROM (
  select max(HorasMes) mes,
         max(HorasAnio) anio
  from siap_ces_tray.ihorasextras
  where PerDocNum='$cedula'
) FC
join siap_ces_tray.ihorasextras M1
  on HorasMes=mes
 and HorasAnio=anio
WHERE PerDocNum='$cedula'
  AND HorExtrasCant>0;

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

	my $sth = portal3::dbGet($dbh, "siap_ces_tray.ihorasextras",
			["HorasFchCarga","count(distinct perdocnum) Personas","count(*) Registros"],
			"HorasFchProc is null and HorasFchCarga is not null",
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

select max(HorasFchCarga) `Fecha de carga`,
       ULT.perdocnum,
       replace(ifnull(concat(PerPriApe,' ',ifnull(PerSegApe,''),', ',PerPriNom,' ',ifnull(PerSegNom,'')),''),char(39),char(44)) nombre,
       count(*) errores
FROM (select HE1.*
      from ihorasextras HE1
      left join ihorasextras HE2
        on HE2.perdocnum=HE1.perdocnum
       and HE2.HorasAnio=HE1.HorasAnio
       and HE2.HorasMes=HE1.HorasMes
       and HE2.HorasId>HE1.HorasId
       and ifnull(HE2.Resultado,'') in ('','OK','ERROR','PE')
       where HE2.HorasId is null
) ULT
left join siap_ces.personas p using (perdocnum)
WHERE resultado='ERROR'
  AND HorasFchCarga >= '2019-03-01'
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
