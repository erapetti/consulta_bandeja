#!/usr/bin/perl
#
# consulta_bandeja.pl
#
#	Consultas de la liquidación de sueldos Corporativo/SIAP


use strict;
use DBI;
use CGI qw/:standard/;
use CGI::Carp qw/fatalsToBrowser/;
use Cache::Memcached;
use portal3 qw/getSession leoPermiso dbDisconnect dbGet myScriptName checkFormat error/;
use Template;

sub dbConnect(;$$$) ;
sub json_response($) ;
sub data2js($) ;
sub corporativo_buscar($$) ;
sub corporativo_certificados($$) ;
sub resumen($);
sub bandeja_buscar($$) ;
sub errores($) ;
sub periodos($) ;
sub multas($$) ;
sub siap_buscar($$) ;
sub siap_suspensiones($$) ;
sub siap_procesos ($) ;
sub coordinacion ($$$) ;
sub periodo_minmax($) ;
sub nombre($$) ;
sub proxy($) ;
sub resumen_posesiones($) ;
sub opcion_corporativo_consulta($$) ;
sub opcion_corporativo_certificados($$) ;
sub opcion_resumen_bandeja($$) ;
sub opcion_consulta_bandeja($$) ;
sub opcion_errores($$) ;
sub opcion_periodos($$) ;
sub opcion_multas($$) ;
sub opcion_siap_consulta($$) ;
sub opcion_siap_suspensiones($$) ;
sub opcion_siap_procesos($$) ;
sub opcion_borrar($$) ;
sub opcion_reliquidar($$) ;

my ($userid,$sessionid) = getSession(cookie(-name=>'SESION'));

my $cedula = checkFormat(param('cedula'), '[\d .-]+');
$cedula =~ s/[^\d]//g;
my $opcion = checkFormat(param('opcion'), '\w+') || 'resumen';

my $page = Template->new(EXPOSE_BLOCKS => 1);
my %tvars;

$tvars{opcion} = $opcion;
$tvars{admin} = ($0 =~ /consulta_bandeja_admin.pl$/);


my $dbh_siap = dbConnect("siap_ces_tray") || error("No se puede establecer una conexión a la base de datos: ".$DBI::errstr,200);
my $dbh_personal = dbConnect("Personal") || error("No se puede establecer una conexión a la base de datos: ".$DBI::errstr,200);

if ($cedula) {
	$tvars{cedula} = $cedula;
	$tvars{nombre} = nombre($dbh_personal,$cedula);
}

my %dispatcher = (
	corporativo => \&opcion_corporativo_consulta,
	certificados => \&opcion_corporativo_certificados,
	resumen => \&opcion_resumen_bandeja,
	consulta => \&opcion_consulta_bandeja,
	errores => \&opcion_errores,
	periodos => \&opcion_periodos,
	multas => \&opcion_multas,
	siap => \&opcion_siap_consulta,
	suspensiones => \&opcion_siap_suspensiones,
	procesos => \&opcion_siap_procesos,
	borrar => \&opcion_borrar,
	reliquidar => \&opcion_reliquidar,
);

my %param = (
	dbh_siap => $dbh_siap,
	dbh_personal => $dbh_personal,
	cedula => (defined($tvars{nombre}) ? $cedula : undef),
);

my $err;

if (defined($dispatcher{$opcion})) {
	$err = &{$dispatcher{$opcion}}(\%param, \%tvars);
}

if (!$err) {
	print header(-charset => 'utf-8');

	# Genero el HTML final:
	$page->process("consulta_bandeja.tt", \%tvars)
	|| die "Template process failed: ", $page->error(), "\n";
}


exit(0);

######################################################################


sub dbConnect(;$$$) {
        my ($base,$user,$pass) = @_;

        my ($db,$host,$dbh);

        $db=$base;
	if ($db eq "Personal") {
		$host="sdb-reader2.ces.edu.uy.";
	} else {
		$host="sdb712-08.ces.edu.uy";
	}
        $user=($user || "consulta_bandeja");
        $pass=($pass || "sdf9d3klj3");

        $dbh = DBI->connect("DBI:mysql:$db:$host:3306",$user,$pass) || return undef;
        $dbh->do("set character set utf8");

        return $dbh;
}

sub json_response($) {
        my ($input) = @_;

	my $out;
	if (ref($input) eq "HASH") {
		$out = "{";
		foreach $_ (keys %$input) {
			$input->{$_} =~ s/"/\\"/g;
			$out .= '"'.$_.'":"'.$input->{$_}.'",';
		}
		$out =~ s/,$//;
		$out .= "}";
		$out =~ s/[\r\n]+/ /g;
	} elsif (ref(\$input) eq "SCALAR") {
		$out = $input;
	} else {
		$out = '{"error":"invalid json_response type '.ref(\$input).'"}';
	}
        
	print header(-charset=>'utf-8',-type=>'application/json'),$out;

        exit(0);
}

sub data2js($) {
	my ($data) = @_;

	return undef if (!defined($data->{data}) || $#{$data->{data}} == -1);

        my $js = "body = Array();\n";
        foreach my $row (@{$data->{data}}) {
                $js .= "body.push( Array('".join("','",@$row)."' ) );\n";
        }
        $js .= "head = Array( {title:'".join("'},{title:'",@{$data->{head}})."'} );\n";

	return $js;
}

sub corporativo_buscar($$) {
	my ($dbh, $cedula) = @_;

	my $sth = $dbh->prepare("

SELECT perdocid,
       FuncAsignadaFchDesde,
       FuncAsignadaFchHasta,
       DependDesc,
       if(AsignDesc<>'',if(DenomCargoDesc<>'DOCENTE',concat(DenomCargoDesc,' ',AsignDesc),AsignDesc),if(DenomCargoDesc='DOCENTE',concat(DenomCargoDesc,':',FuncionDesc),DenomCargoDesc)),
       suplencias,
       RelLabCicloPago,
       format(sum(CargaHorariaCantHoras),2)
FROM v_funciones_del_personal v

-- suplencias, reservas de cargo, etc:
LEFT JOIN (
       select s.RelLabId,group_concat(distinct concat(SuplCausDesc,': ',ifnull(date(SuplFchAlta),'1000-01-01'),' a ',date(RLsupl.RelLabCeseFchReal)) order by RLsupl.RelLabVacanteFchPubDesde separator '<br>') suplencias
       from SUPLENCIAS s
       join SUPLENCIAS_CAUSALES using (SuplCausId)
       join RELACIONES_LABORALES RLtit using (RelLabId)
       join RELACIONES_LABORALES RLsupl on RLsupl.SillaId=RLtit.SillaId and RLsupl.RelLabVacantePrioridad=RLtit.RelLabVacantePrioridad+1
       where SuplCausId in (6,7,10,15)
         and (RLsupl.RelLabVacanteFchPubDesde is null or date(RLsupl.RelLabVacanteFchPubDesde)<=date(RLsupl.RelLabCeseFchReal))
	 and year(RLsupl.RelLabCeseFchReal)>=year(curdate())
       group by 1
) S ON S.RelLabId=v.RelLabId

WHERE perdocid='".$cedula."'
  AND (FuncAsignadaFchHasta>='2018-03-01' OR FuncAsignadaFchHasta='1000-01-01')
  AND (FuncAsignadaFchDesde<=FuncAsignadaFchHasta OR FuncAsignadaFchHasta='1000-01-01')
GROUP BY 1,2,3,4,5,6,7
ORDER BY 2,4,5,6,3;

	");
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","Cargo/Asignatura","Observaciones","Ciclo","Horas"], data=>$rows};
}

sub corporativo_certificados($$) {
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
  AND CertFchIni>=$desde
ORDER BY 1,2;

	");
	$sth->execute();

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","Tipo","Observaciones"], data=>$rows};
}

sub resumen($) {
	my ($dbh) = @_;

	my $sth = dbGet($dbh, "siap_ces_tray.ihorasclase",
			["DesFchCarga","count(distinct perdocnum) Personas","count(*) Registros"],
			"HorClaEmpCod=1 and DesFchProc is null and desfchcarga is not null",
			"group by 1 order by 1"
	               );

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Fecha Carga","Personas","Registros"], data=>$rows};
}

sub bandeja_buscar($$) {
	my ($dbh, $cedula) = @_;

	my $sth = $dbh->prepare("

SELECT HC3.perdocnum,
       HC3.HorClaFchPos FchPos,
       HC3.HorClaFchCese FchCese,
       InsDsc Dependencia,
       AsiNom Asign,
       HC3.HorClaCic Ciclo,
       sum(HC3.horclahor) Horas,
       HC3.desfchcarga,
       ifnull(HC3.DesFchProc,'') DesFchProc,
       concat(if(HC3.HorClaBajLog,'Anulado: ',''),if(HC3.DesFchProc is null and HC3.mensaje='','Pendiente',left(replace(HC3.mensaje,'ERROR: ',''),60))) mensaje
FROM (select HC1.perdocnum,
             HC1.desfchcarga,
             HC1.DesFchProc,
             HC1.nrolote
      from ihorasclase HC1
      left join ihorasclase HC2
             on HC2.perdocnum=HC1.perdocnum
            and (HC1.desfchcarga<HC2.desfchcarga
                 or
                 HC1.desfchcarga=HC2.desfchcarga and isnull(HC2.DesFchProc) and (HC1.DesFchProc is not null)
                )
      where HC1.perdocnum='".$cedula."'
        and HC1.desfchcarga is not null
        and HC2.perdocnum is null
      limit 1) ULT
JOIN ihorasclase HC3
  ON HC3.perdocnum=ULT.perdocnum
 AND ((ULT.DesFchProc IS NOT NULL AND HC3.DesFchProc=ULT.DesFchProc AND (HC3.nrolote IS NULL OR HC3.nrolote=ULT.nrolote))
      OR
      (ULT.DesFchProc IS NULL AND HC3.DesFchProc IS NULL AND HC3.desfchcarga=ULT.desfchcarga)
     )
JOIN siap_ces.institucionales
  ON HorClaInsCod=InsCod
LEFT JOIN siap_ces.asignaturas
  ON AsiCod=HC3.HorClaAsiCod
WHERE NOT(HC3.HorClaBajLog=1 AND HC3.HorClaCauBajCod=99)
GROUP BY 1,2,3,4,5,6,8,9,10
ORDER BY 2,4,5,6,3;

	");
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","Asignatura","Ciclo","Horas","FchCarga","FchProc","Mensaje"], data=>$rows};
}

sub errores($) {
	my ($dbh) = @_;

	my $sth = $dbh->prepare("

select HC1.perdocnum Cédula,HC1.desfchcarga `Fecha de carga`,count(*) errores
from ihorasclase HC1
left join ihorasclase HC2
  on HC1.perdocnum=HC2.perdocnum 
 and HC1.desfchcarga<HC2.desfchcarga
where HC2.perdocnum is null
  and not(HC1.HorClaBajLog=1 and HC1.HorClaCauBajCod=99)
  and HC1.desfchcarga >= '2018-03-01'
  and HC1.resultado not in ('OK','')
group by 1

	");
	$sth->execute;

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Fecha Carga","Errores"], data=>$rows};
}

sub periodos($) {
	my ($dbh) = @_;

	my $sth = $dbh->prepare("

SELECT desde,hasta
FROM siap_ces_tray.periodos
ORDER BY id

	");
	$sth->execute;

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Desde","Hasta"], data=>$rows};
}

sub multas($$) {
	my ($dbh,$cedula) = @_;

	my $SQL= "

SELECT PerDocNum Cédula,
       MultAnio,
       MultMes,
       InsDsc,
       case RubroCod when '81117' then 'ND' when '81119' then 'PA' when '81131' then 'DI' when '81118' then 'DD' else NULL end tipo,
       MultCic,
       concat(if(MultCantDias>0,concat(MultCantDias,' D'),''),if(MultCantHor>0,concat(MultCantHor,' H'),'')) DiasHoras,
       MultFchCarga,
       MultFchProc,
       Mensaje
FROM (
  select month(max(MultFchCarga)) mes,
         year(max(MultFchCarga)) anio
  from siap_ces_tray.imultas
) FC
join siap_ces_tray.imultas M1
  on month(MultFchCarga)=mes
 and year(MultFchCarga)=anio
join siap_ces.institucionales
  on InsCod=MultInsCod
WHERE PerDocNum='$cedula'
  AND (MultCantDias>0 OR MultCantHor>0);

	";
        my $sth = $dbh->prepare($SQL);
        $sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Año","Mes","Dependencia","Tipo","Ciclo","Días/Horas","FchCarga","FchProc","Mensaje"], data=>$rows};
}

sub siap_buscar($$) {
	my ($dbh, $cedula) = @_;

	my $sth = $dbh->prepare("

SELECT perdocnum,
       DesFchIng,
       DesFchEgr,
       InsDsc,
       ifnull(AsiNom,cargo),
       group_concat(concat(ConDsc,': ',DesConFchDes,' a ',DesConFchHas)) reservas,
       CicCod,
       sum(horas)
FROM siap_ces.v_designaciones v
LEFT JOIN siap_ces.asignaturas using (AsiCod)
LEFT JOIN siap_ces.designacionesconceptos dc
     ON dc.DesConEmpCod = v.EmpCod
    AND dc.DesConCarNum = v.CarNum
    AND dc.DesConCarNumVer = v.CarNumVer
    AND dc.ConCod in (81150,81151,81154,81155,81156)
    AND (dc.DesConFchHas >= '2018-01-01' or dc.DesConFchHas='1000-01-01')
LEFT JOIN siap_ces.conceptos using (ConCod)
WHERE perdocnum='".$cedula."'
  AND (DesFchEgr='1000-01-01' OR DesFchEgr>='2018-03-01')
GROUP BY 1,2,3,4,5,7
ORDER BY 2,4,5,7,3;

	");
	$sth->execute();

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","Cargo/Asignatura","Observaciones","Ciclo","Horas"], data=>$rows};
}

sub siap_suspensiones($$) {
	my ($dbh, $cedula) = @_;

	my $desde = "concat(year(curdate())+if(month(curdate())<3,-1,0),'-03-01')";

	my $SQL = "
SELECT PerDocNum,
       Desde,
       Hasta,
       Motivo
FROM siap_ces_tray.suspensiones
WHERE PerDocNum='$cedula'
  AND desde>=$desde
";
	my $sth = $dbh->prepare($SQL);
	$sth->execute() or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Motivo"], data=>$rows};
}

sub siap_procesos ($) {
	my ($dbh) = @_;


	my $SQL = "
select PrcDsc,
       PrcFchIni,
       concat(round(100*PrcAct/PrcTot,2),' %') avance,
       if(time_to_sec(timediff(now(),ifnull(PrcFchUltAct,'1000-01-01')))>600,'Estancado','Activo') estado
from siap_ces.procesos
where PrcTot>PrcAct
  and PrcCancelado=0
  and PrcEstado=1
";
	my $sth = $dbh->prepare($SQL);
	$sth->execute() or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Proceso","Desde","Avance","Estado"], data=>$rows};
}

sub coordinacion ($$$) {
	my ($dbh, $cedula, $data) = @_;

	my $SQL = "

SELECT 0 DependId,CicloDePago,HrsCoordinacionFechaAlta,date_add(HrsCoordinacionFechaAlta, interval 1 day) manana,format(sum(HrsCoordConSigno),2)
FROM HORAS_COORDINACION join Personas.PERSONASDOCUMENTOS on coordperid=perid and paiscod='UY' and doccod='CI'
WHERE perdocid='".$cedula."'
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

sub periodo_minmax($) {
	my ($dbh) = @_;

	my $SQL = "

SELECT min(desde),max(hasta)
FROM periodos
WHERE desde >= concat(year(curdate())+if(month(curdate())>=3,0,-1),'-03-01')
  AND hasta <  concat(year(curdate())+if(month(curdate())>=3,1,0),'-03-01')

";

	my $sth = $dbh->prepare($SQL);
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my @row = $sth->fetchrow_array;
	$sth->finish;

	return ($row[0], $row[1]);
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

sub proxy($) {
  my ($url) = @_;

  use LWP::UserAgent;
  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(GET => "$url");
  my $rsp = $ua->request($req);
  return ($rsp->code, $rsp->decoded_content);
}

sub resumen_posesiones($) {
  my ($rdata) = @_;
  my $html;

  # Busco en qué columnas vienen los datos que me interesan:
  my ($colciclo, $colhoras, $coldesde, $colhasta, $colobs);
  for my $i (0..$#{$rdata->{head}}) {
	if ($rdata->{head}[$i] eq "Ciclo") {
		$colciclo = $i;
	} elsif ($rdata->{head}[$i] eq "Horas") {
		$colhoras = $i;
	} elsif ($rdata->{head}[$i] eq "Desde") {
		$coldesde = $i;
	} elsif ($rdata->{head}[$i] eq "Hasta") {
		$colhasta = $i;
	} elsif ($rdata->{head}[$i] eq "Observaciones") {
		$colobs = $i;
	}
  }

  my @time = localtime(time());
  my $now = sprintf "%4d-%02d-%02d", $time[5]+1900, $time[4]+1, $time[3];
  my $total = 0;
  my %total;
  # Recorro las designaciones:
  foreach $_ (@{$rdata->{data}}) {
	if (($_->[$colhasta] eq "" || $_->[$colhasta] ge $now || $_->[$colhasta] eq "1000-01-01") &&
	    ($_->[$coldesde] eq "" || $_->[$coldesde] le $now || $_->[$coldesde] eq "1000-01-01")) {
		# Encontré una designación que está en fecha
		my $obs = $_->[$colobs];
		my $reserva = 0;

		while(!$reserva && $obs && $obs =~ s/reserva[^\d]*: (\d\d\d\d-\d\d-\d\d) a (\d\d\d\d-\d\d-\d\d)//i) {
			# Encontré una reserva de cargo que tengo que verificar que está en fecha
			my $desde = $1;
			my $hasta = $2;

			$reserva = ($hasta ge $now || $hasta eq "1000-01-01") && ($desde le $now || $desde eq "1000-01-01");
		}
		if (!$reserva) {
			$total{$_->[$colciclo]} += $_->[$colhoras];
			$total += $_->[$colhoras];
		}
	}
  }
  if (defined($total{""})) {
	$total{0} += $total{""};
	delete $total{""};
  }
  $page->process("consulta_bandeja.tt/block_resumen_posesiones", {c0=>$total{0},c1=>$total{1},c2=>$total{2},c3=>$total{3},c4=>$total{4},c5=>$total{5},total=>$total}, \$html);

  return $html;
}

######################################################################
#
# Opciones del menú
#

sub opcion_corporativo_consulta($$) {
	my ($rparam, $rtvars) = @_;

	my $cedula = $rparam->{cedula};
	my $dbh_personal = $rparam->{dbh_personal};

	if (defined($cedula)) {
		my $corp = corporativo_buscar($dbh_personal, $cedula);

		# agrego las horas de coordinación
		coordinacion($dbh_personal, $cedula, $corp->{data});

		$rtvars->{js} = data2js($corp);
		$rtvars->{subtitulo} = "Designaciones:";

		$rtvars->{resumen_posesiones} = resumen_posesiones($corp);

	}

	$rtvars->{titulo} = "Consulta al Sistema Corporativo";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_corporativo_certificados($$) {
	my ($rparam, $rtvars) = @_;

	my $cedula = $rparam->{cedula};
	my $dbh_personal = $rparam->{dbh_personal};

	if (defined($cedula)) {
		my $corp = corporativo_certificados($dbh_personal, $cedula);

		$rtvars->{js} = data2js($corp);
		$rtvars->{subtitulo} = "Certificados Médicos:";
	}

	$rtvars->{titulo} = "Consulta de certificados médicos en el Sistema Corporativo";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_resumen_bandeja($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $resumen = resumen($dbh_siap);

	$rtvars->{js} = data2js($resumen);
	$rtvars->{titulo} = "Datos pendientes en la bandeja";

	return 0;
}

sub opcion_consulta_bandeja($$) {
	my ($rparam, $rtvars) = @_;

	my $cedula = $rparam->{cedula};
	my $dbh_siap = $rparam->{dbh_siap};

	if (defined($cedula)) {
		my $resultado = bandeja_buscar($dbh_siap, $cedula);

		(defined($resultado)) || error($DBI::errstr,200);

		if ($#{$resultado->{data}} >= 0) {

			$rtvars->{js} = data2js($resultado);
			$rtvars->{subtitulo} = "Últimos datos en la bandeja:";

			# esto va para afuera del if:
			$rtvars->{resumen_posesiones} = resumen_posesiones($resultado);
		}

		if ($rtvars->{admin} && defined($tvars{nombre}) && $tvars{nombre} ne '') {
			# busco pendientes para definir si habilito borrar o reliquidar:
			my $pendientes = 0;
			foreach my $row (@{$resultado->{data}}) {
				$row->[$#{$row}] =~ /Pendiente/ and $pendientes = 1 and last;
			}

			if ($pendientes) {
				$rtvars->{btn} = 'Borrar pendientes';
				$rtvars->{btn_class} = 'btn-danger';
				$rtvars->{modal_title} = 'Borrar pendientes';
				$rtvars->{modal_body} = 'Esta operación va a borrar la liquidación pendiente para este docente';
				$rtvars->{modal_processing} = 'Borrando';
				$rtvars->{modal_button} = 'Borrar';
				$rtvars->{modal_opcion} = 'borrar';
			} else {
				$rtvars->{btn} = 'Reliquidar';
				$rtvars->{btn_class} = 'btn-primary';
				$rtvars->{modal_title} = 'Reliquidar';
				$rtvars->{modal_body} = 'Esta operación va a generar una nueva liquidación para este docente';
				$rtvars->{modal_processing} = 'Reliquidando';
				$rtvars->{modal_button} = 'Reliquidar';
				$rtvars->{modal_opcion} = 'reliquidar';
			}

		}
	}

	$rtvars->{titulo} = "Consulta a la bandeja de docencia directa";
	$rtvars->{buscador_cedulas} = 1;

	# Pongo fondo rojo en las filas que no tienen ciclo de pago (columna 5)
	# y muestro los botones de borrar o reliquidar
	$rtvars->{data_table_options} = '
	  createdRow: function(row, data, dataIndex) {
		if(data[5] ==  "") {
			$(row).addClass("alert alert-danger");
		}
	  },
	  drawCallback: function(settings) {
		$(".delayed").show();
	  },
	';

	return 0;
}

sub opcion_errores($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $errores = errores($dbh_siap);

	$rtvars->{js} = data2js($errores);
	$rtvars->{titulo} = "Personas con último pasaje en error";

	if ($#{$errores->{data}} > -1) {
		# Agrego enlaces en la primer columna, para acceder a la consulta de esa cédula
		$rtvars->{js} .= "
\$('table#maintable').on( 'draw.dt', function () {
  \$('table#maintable tbody tr td:first-child').addClass('link');
  \$('table#maintable tbody tr td:first-child').click(function(){
      window.location.href = '?opcion=consulta&cedula='+\$(this).text();
  });
});

		";
	}

	return 0;
}

sub opcion_periodos($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $periodos = periodos($dbh_siap);

	$rtvars->{js} = data2js($periodos);
	$rtvars->{titulo} = "Períodos de liquidación";

	return 0;
}

sub opcion_multas($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $cedula = $rparam->{cedula};

	if (defined($cedula)) {

		my $multas = multas($dbh_siap,$cedula);

		$rtvars->{js} = data2js($multas);
		$rtvars->{subtitulo} = "Datos del último mes en la bandeja";
	}

	$rtvars->{titulo} = "Bandeja de multas";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_siap_consulta($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $cedula = $rparam->{cedula};

	if (defined($cedula)) {

		my $siap = siap_buscar($dbh_siap, $cedula);

		$rtvars->{js} = data2js($siap);
		$rtvars->{subtitulo} = "Últimos datos en SIAP";

		$rtvars->{resumen_posesiones} = resumen_posesiones($siap);
	}

	$rtvars->{titulo} = "Consulta a SIAP";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_siap_suspensiones($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $cedula = $rparam->{cedula};

	if (defined($cedula)) {
		my $suspensiones = siap_suspensiones($dbh_siap, $cedula);

		$rtvars->{js} = data2js($suspensiones);
		$rtvars->{subtitulo} = "Suspensiones:";
	}

	$rtvars->{titulo} = "Consulta de marcas de suspensión de pagos en SIAP";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_siap_procesos($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $procesos = siap_procesos($dbh_siap);

	$rtvars->{js} = data2js($procesos);
	$rtvars->{titulo} = "Procesos activos en SIAP";

	return 0;
}

sub opcion_borrar($$) {
	my ($rparam, $rtvars) = @_;

	my $cedula = $rparam->{cedula};

	if (!$cedula) {
		json_response({error=>"Parámetros incorrectos"});
	}

	my ($code, $text) = proxy("http://ssueldos01.ces.edu.uy/cgi-bin/sueldos/borrar.pl?cedula=$cedula");
	if ($code == 200) {
		json_response($text);
	} else {
		json_response({error=>"$text",code=>$code});
	}
	return 1;
}

sub opcion_reliquidar($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $cedula = $rparam->{cedula};

	my ($desde,$hasta) = periodo_minmax($dbh_siap);

	if (!$desde || !$hasta) {
		json_response({error=>"No se puede obtener el período de reliquidación desde la base de datos: ".$DBI::errstr});
		return 1;
	}
	if (!$cedula) {
		json_response({error=>"Parámetros incorrectos"});
	}

	my ($code, $text) = proxy("http://ssueldos01.ces.edu.uy/cgi-bin/sueldos/reliquidar.pl?desde=$desde\&hasta=$hasta\&cedula=$cedula");
	if ($code == 200) {
		json_response($text);
	} else {
		json_response({error=>"$text",code=>$code});
	}
	return 1;
}
