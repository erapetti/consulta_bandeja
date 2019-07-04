use strict;

package multas;

sub buscar($$) {
	my ($dbh,$cedula) = @_;

	my $SQL= "

SELECT PerDocNum Cédula,
       MultAnio,
       MultMes,
       InsDsc,
       case RubroCod
         when '81117' then 'ND'
         when '381117' then 'ND-MG'
         when '81119' then 'PA'
         when '381119' then 'PA-MG'
         when '81131' then 'DI'
         when '381131' then 'DI-MG'
         when '81118' then 'DD'
         when '381118' then 'DD-MG'
         else RubroCod end tipo,
       MultCic,
       concat(if(MultCantDias<>0,concat(MultCantDias,' D'),''),if(MultCantHor<>0,concat(MultCantHor,' H'),'')) DiasHoras,
       MultFchCarga,
       MultFchProc,
       Mensaje
FROM siap_ces_tray.imultas M1
JOIN siap_ces.institucionales
  ON InsCod=MultInsCod
WHERE PerDocNum='$cedula'
  AND (MultCantDias<>0 OR MultCantHor<>0)
  AND (MultAnio = year(curdate()) or month(curdate())<3 and MultAnio = year(curdate())-1)

	";
        my $sth = $dbh->prepare($SQL);
        $sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Año","Mes","Dependencia","Tipo","Ciclo","Días/Horas","FchCarga","FchProc","Mensaje"], data=>$rows};
}

sub resumen($) {
	my ($dbh) = @_;

	my $sth = portal3::dbGet($dbh, "siap_ces_tray.imultas",
			["MultFchCarga","count(distinct perdocnum) Personas","count(*) Registros"],
			"MultFchProc is null and MultFchCarga is not null",
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

select max(MultFchCarga) `Fecha de carga`,
       ULT.perdocnum,
       replace(ifnull(concat(PerPriApe,' ',ifnull(PerSegApe,''),', ',PerPriNom,' ',ifnull(PerSegNom,'')),''),char(39),char(44)) nombre,
       count(*) errores
FROM (select M1.*
      from imultas M1
      left join imultas M2
        on M2.PerDocTpo=M1.PerDocTpo
       and M2.PerDocPaisCod=M1.PerDocPaisCod
       and M2.PerDocNum=M1.PerDocNum
       and M2.MultCarNum=M1.MultCarNum
       and M2.MultInsCod=M1.MultInsCod
       and M2.MultAnio=M1.MultAnio
       and M2.MultMes=M1.MultMes
       and M2.RubroCod=M1.RubroCod
       and M2.MultId>M1.MultId
       where M2.MultId is null
         and M1.MultFchCarga >= '2019-03-01'
) ULT
left join siap_ces.personas p using (perdocnum)
WHERE resultado='ERROR'
  AND MultFchCarga >= '2019-03-01'
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
