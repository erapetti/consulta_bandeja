use strict;

package multas;

sub buscar($$) {
	my ($dbh,$cedula) = @_;

	my $SQL= "

SELECT PerDocNum Cédula,
       MultAnio,
       MultMes,
       InsDsc,
       case RubroCod when '81117' then 'ND' when '81119' then 'PA' when '81131' then 'DI' when '81118' then 'DD' else NULL end tipo,
       MultCic,
       concat(if(MultCantDias<>0,concat(MultCantDias,' D'),''),if(MultCantHor<>0,concat(MultCantHor,' H'),'')) DiasHoras,
       MultFchCarga,
       MultFchProc,
       Mensaje
FROM (
  select month(max(MultFchCarga)) mes,
         year(max(MultFchCarga)) anio
  from siap_ces_tray.imultas
  where PerDocNum='$cedula'
) FC
join siap_ces_tray.imultas M1
  on month(MultFchCarga)=mes
 and year(MultFchCarga)=anio
join siap_ces.institucionales
  on InsCod=MultInsCod
WHERE PerDocNum='$cedula'
  AND (MultCantDias<>0 OR MultCantHor<>0);

	";
        my $sth = $dbh->prepare($SQL);
        $sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Año","Mes","Dependencia","Tipo","Ciclo","Días/Horas","FchCarga","FchProc","Mensaje"], data=>$rows};
}

1;
