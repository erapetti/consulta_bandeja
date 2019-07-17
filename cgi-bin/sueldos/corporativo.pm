use strict;

package corporativo;

sub horas_por_periodo($$) {
	my ($dbh, $cedula) = @_;
	
	my @time = localtime(time());
	my $year = $time[5]+1900;
	if ($time[4]+1 < 3) {
		# enero y febrero
		$year--;
	}
	my $desde = sprintf "%4d-03-01", $year;
	my $hasta = sprintf "%4d-02-28", $year+1;

	my $sth = $dbh->prepare("

call sp_horas_por_periodo((select perid from Personas.V_PERSONAS where perdocid='$cedula'),'$desde','$hasta',0,0,0,0);

	");
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return "ERROR: $DBI::errstr";

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return $rows;
}

sub buscar($$) {
	my ($dbh, $cedula) = @_;

	my $sth = $dbh->prepare("

SELECT perdocid,
       FuncAsignadaFchDesde desde,
       FuncAsignadaFchHasta hasta,
       DependDesc,
       if(RelLabDesignCaracter='S','',ifnull(Correlativo,'')) Correlativo,
       group_concat(v.RelLabId separator ', ') RLs,
       RelLabDesignCaracter,
       if(AsignDesc<>'',if(DenomCargoDesc<>'DOCENTE',concat(DenomCargoDesc,' ',AsignDesc),AsignDesc),if(DenomCargoDesc='DOCENTE',concat(DenomCargoDesc,':',FuncionDesc),DenomCargoDesc)) CargoAsignatura,
       suplencias,
       RelLabCicloPago,
       format(sum(FuncRelLabCantHrs),2) horas
FROM v_funciones_del_personal v
LEFT JOIN PUESTOS using (PuestoId)

-- suplencias, reservas de cargo, etc:
LEFT JOIN (
       select s.RelLabId,
              group_concat(
                           distinct(
                             concat(SuplCausDesc,': ',greatest(RLtit.RelLabFchIniActividades,adddate(ifnull(date(SuplFchAlta),'1000-01-01'),1)),' a ',date(ifnull(RLsupl.RelLabCeseFchReal,ifnull(RLtit.RelLabCeseFchReal,'1000-01-01'))))
                           )
                           order by RLsupl.RelLabVacanteFchPubDesde
                           separator '<br>'
              ) suplencias
       from SUPLENCIAS s
       join SUPLENCIAS_CAUSALES using (SuplCausId)
       join RELACIONES_LABORALES RLtit using (RelLabId)
       -- join RELACIONES_LABORALES RLsupl on RLsupl.SillaId=RLtit.SillaId and RLsupl.RelLabVacantePrioridad=RLtit.RelLabVacantePrioridad+1
       join RELACIONES_LABORALES RLsupl on RLsupl.RelLabId=s.SuplRelLabId
       where SuplCausId in (6,7,15,16,17,20,39,43,162,42,40)
         and (RLsupl.RelLabVacanteFchPubDesde is null
              or RLsupl.RelLabCeseFchReal is null
              or date(RLsupl.RelLabVacanteFchPubDesde)<=date(RLsupl.RelLabCeseFchReal)
         )
	 and (RLsupl.RelLabCeseFchReal is null
              or year(RLsupl.RelLabCeseFchReal) >= (year(curdate()) - if(month(curdate())<3,1,0))
         )
         and RLsupl.RelLabAnulada=0
         and greatest(RLtit.RelLabFchIniActividades,adddate(ifnull(date(SuplFchAlta),'1000-01-01'),1)) <= ifnull(RLsupl.RelLabCeseFchReal,ifnull(RLtit.RelLabCeseFchReal,'1000-01-01'))

       group by 1
) S ON S.RelLabId=v.RelLabId

WHERE perdocid='".$cedula."'
  AND (FuncAsignadaFchHasta>='2019-03-01' OR FuncAsignadaFchHasta='1000-01-01')
  AND (FuncAsignadaFchDesde<=FuncAsignadaFchHasta OR FuncAsignadaFchHasta='1000-01-01')
GROUP BY 1,2,3,4,5,7,8,9
ORDER BY 2,4,7,8,3;

	");
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","Correlativo","RelLab","Carácter","Cargo/Asignatura","Observaciones","Ciclo","Horas"], data=>$rows};
}

sub certificados($$) {
	my ($dbh, $cedula) = @_;

	my $desde = "concat(year(curdate())+if(month(curdate())<3,-1,0),'-03-01')";
	my $sth = $dbh->prepare("

SELECT concat(cedula,digito) cedula,
       CertFchIni,
       CertFchFin,
       DependDesc,
       codlic,
       observaciones
FROM certificaciones_anep
WHERE cedula=left('$cedula',length('$cedula')-1)
  AND digito=right('$cedula',1)
  AND (CertFchIni>=$desde OR CertFchFin>=$desde)
ORDER BY 1,2;

	");
	$sth->execute();

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","Tipo","Observaciones"], data=>$rows};
}

sub coordinacion ($$$) {
	my ($dbh, $cedula, $data) = @_;

	my $SQL = "

SELECT 0 DependId,CicloDePago,HrsCoordinacionFechaAlta,date_add(HrsCoordinacionFechaAlta, interval 1 day) manana,format(sum(HrsCoordConSigno),2)
FROM HORAS_COORDINACION join Personas.PERSONASDOCUMENTOS on coordperid=perid and paiscod='UY' and doccod='CI'
WHERE perdocid='".$cedula."'
  AND HrsCoordinacionFechaAlta>='2019-03-01'
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
";

	my $sth = $dbh->prepare($SQL);
	$sth->execute();

	(defined($sth)) or return undef;

	my (%coord);
	while (my @row = $sth->fetchrow_array) {
		if (!defined($coord{$row[0]}{$row[1]})) {
			$coord{$row[0]}{$row[1]} = { horas => $row[4], desde => $row[2] };
		} else {
			push @{$data}, [$cedula, $coord{$row[0]}{$row[1]}{desde}, $row[2], '', 'COORDINACION', '', $row[1], $coord{$row[0]}{$row[1]}{horas}];
			if ($coord{$row[0]}{$row[1]}{horas} + $row[4] > 0) {
				$coord{$row[0]}{$row[1]}{horas} += $row[4];
				$coord{$row[0]}{$row[1]}{desde} = $row[3];
			} elsif ($coord{$row[0]}{$row[1]}{horas} + $row[4] == 0) {
				delete $coord{$row[0]}{$row[1]};
			}
		}
		
	}
	foreach my $dependid (keys %coord) {
		foreach my $cicloid (keys %{$coord{$dependid}}) {
			push @{$data}, [$cedula, $coord{$dependid}{$cicloid}{desde}, '', '', 'COORDINACION', '', $cicloid, $coord{$dependid}{$cicloid}{horas}];
		}
	}

	$sth->finish;
}

sub nombre($$) {
	my ($dbh, $cedula) = @_;

	my $SQL = "

SELECT PerNombreCompleto
FROM Personas.PERSONAS
JOIN Personas.PERSONASDOCUMENTOS USING (PerId)
WHERE paiscod='UY'
  AND doccod='CI'
  AND perdocid='".$cedula."'

";

	my $sth = $dbh->prepare($SQL);
	$sth->execute();

	(defined($sth)) or return undef;

	my @row = $sth->fetchrow_array;
	$sth->finish;

	return $row[0];
}

1;
