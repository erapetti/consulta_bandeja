use strict;

package irpfabonos;

sub buscar($$) {
	my ($dbh,$cedula) = @_;

	my $SQL= "

SELECT PerDocNum Cédula,
       VarAnio,
       VarMes,
       VarImporte,
       VarFchCarga,
       VarFchProc,
       Mensaje
FROM siap_ces_tray.ivariables M1
WHERE PerDocNum='$cedula'
  AND VarTipoVariable='IRPF-ABONOS'
  AND VarConNum=740016
  AND (VarAnio = year(curdate()) or month(curdate())<3 and VarAnio = year(curdate())-1)

	";
        my $sth = $dbh->prepare($SQL);
        $sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Año","Mes","Importe","FchCarga","FchProc","Mensaje"], data=>$rows};
}

sub resumen($) {
	my ($dbh) = @_;

	my $sth = portal3::dbGet($dbh, "siap_ces_tray.ivariables",
			["VarFchCarga","count(distinct perdocnum) Personas","count(*) Registros"],
			"VarFchProc is null and VarFchCarga is not null and VarTipoVariable='IRPF-ABONOS'",
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

select max(VarFchCarga) `Fecha de carga`,
       ULT.perdocnum,
       replace(ifnull(concat(PerPriApe,' ',ifnull(PerSegApe,''),', ',PerPriNom,' ',ifnull(PerSegNom,'')),''),char(39),char(44)) nombre,
       count(*) errores
FROM (select M1.*
      from ivariables M1
      left join ivariables M2
        on M2.PerDocTpo=M1.PerDocTpo
       and M2.PerDocPaisCod=M1.PerDocPaisCod
       and M2.PerDocNum=M1.PerDocNum
       and M2.VarCarNum=M1.VarCarNum
       and M2.VarAnio=M1.VarAnio
       and M2.VarMes=M1.VarMes
       and M2.VarConNum=M1.VarConNum
       and M2.VarId>M1.VarId
       and M2.VarTipoVariable=M1.VarTipoVariable
       where M2.VarId is null
         and M1.VarFchCarga >= '2019-03-01'
) ULT
left join siap_ces.personas p using (perdocnum)
WHERE resultado='ERROR'
  AND VarFchCarga >= '2019-03-01'
  AND VarTipoVariable='IRPF-ABONOS'
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
