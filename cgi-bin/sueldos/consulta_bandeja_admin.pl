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
use Template;
use portal3 qw/getSession leoPermiso dbDisconnect dbGet myScriptName checkFormat error/;
use periodos;
use corporativo;
use bandeja_dd;
use bandeja_di;
use horasextras;
use viaticos;
use multas;
use siap;

sub dbConnect(;$$$) ;
sub json_response($) ;
sub data2js($) ;
sub proxy($) ;
sub resumen_posesiones($) ;
sub opcion_corporativo_consulta($$) ;
sub opcion_corporativo_certificados($$) ;
sub opcion_bandejadd_consulta($$) ;
sub opcion_bandejadd_resumen($$) ;
sub opcion_bandejadd_errores($$) ;
sub opcion_bandejadd_periodos($$) ;
sub opcion_bandejadi_consulta($$) ;
sub opcion_bandejadi_resumen($$) ;
sub opcion_bandejadi_errores($$) ;
sub opcion_bandejadi_periodos($$) ;
sub opcion_horasextras_consulta($$) ;
sub opcion_horasextras_resumen($$) ;
sub opcion_horasextras_errores($$) ;
sub opcion_horasextras_periodos($$) ;
sub opcion_viaticos_consulta($$) ;
sub opcion_multas_consulta($$) ;
sub opcion_multas_resumen($$) ;
sub opcion_multas_errores($$) ;
sub opcion_siap_consulta($$) ;
sub opcion_siap_suspensiones($$) ;
sub opcion_siap_procesos($$) ;
sub ajax_borrar($$) ;
sub ajax_reliquidar($$) ;
sub ajax_cargar($$) ;

my ($userid,$sessionid) = getSession(cookie(-name=>'SESION'));

my $cedula = checkFormat(param('cedula'), '[\d .-]+');
$cedula =~ s/[^\d]//g;
my $opcion = checkFormat(param('opcion'), '\w+') || 'resumen';
my $bandeja = checkFormat(param('bandeja'), '(dd|di|he|vi)');

my $page = Template->new(EXPOSE_BLOCKS => 1);
my %tvars;

$tvars{opcion} = $opcion;
$tvars{admin} = ($0 =~ /consulta_bandeja_admin.pl$/);
$tvars{bandeja} = $bandeja;


my $dbh_siap = dbConnect("siap_ces_tray") || error("No se puede establecer una conexión a la base de datos: ".$DBI::errstr,200);
my $dbh_personal = dbConnect("Personal") || error("No se puede establecer una conexión a la base de datos: ".$DBI::errstr,200);

if ($cedula) {
	$tvars{cedula} = $cedula;
	$tvars{nombre} = corporativo::nombre($dbh_personal,$cedula);
}

my @opciones = (
        {titulo=>'Corporativo', items=> [
		{opcion=>'corporativo', titulo=>'Consulta', icono=>'fas fa-search', funcion=>\&opcion_corporativo_consulta},
		{opcion=>'certificados', titulo=>'Certificados Médicos', icono=>'fas fa-medkit', funcion=>\&opcion_corporativo_certificados},
	]},
	{titulo=>'Bandeja de DD', items=> [
		{opcion=>'consultaDD', titulo=>'Consulta', icono=>'fas fa-search', funcion=>\&opcion_bandejadd_consulta},
		{opcion=>'resumenDD', titulo=>'Pendientes', icono=>'fas fa-circle', funcion=>\&opcion_bandejadd_resumen},
		{opcion=>'erroresDD', titulo=>'Errores', icono=>'fas fa-times-circle', funcion=>\&opcion_bandejadd_errores},
		{opcion=>'periodosDD', titulo=>'Períodos', icono=>'far fa-calendar-alt', funcion=>\&opcion_bandejadd_periodos},
	]},
	{titulo=>'Bandeja de DI', items=> [
		{opcion=>'consultaDI', titulo=>'Consulta', icono=>'fas fa-search', funcion=>\&opcion_bandejadi_consulta},
		{opcion=>'resumenDI', titulo=>'Pendientes', icono=>'fas fa-circle', funcion=>\&opcion_bandejadi_resumen},
		{opcion=>'erroresDI', titulo=>'Errores', icono=>'fas fa-times-circle', funcion=>\&opcion_bandejadi_errores},
		{opcion=>'periodosDI', titulo=>'Períodos', icono=>'far fa-calendar-alt', funcion=>\&opcion_bandejadi_periodos},

		{opcion=>'reliquidar', funcion=>\&ajax_reliquidar},
		{opcion=>'borrar', funcion=>\&ajax_borrar},
	]},
	{titulo=>'Bandeja de Horas Extras', items=> [
		{opcion=>'consultaHE', titulo=>'Consulta', icono=>'fas fa-search', funcion=>\&opcion_horasextras_consulta},
		{opcion=>'resumenHE', titulo=>'Pendientes', icono=>'fas fa-circle', funcion=>\&opcion_horasextras_resumen},
		{opcion=>'erroresHE', titulo=>'Errores', icono=>'fas fa-times-circle', funcion=>\&opcion_horasextras_errores},
		{opcion=>'periodosHE', titulo=>'Períodos', icono=>'far fa-calendar-alt', funcion=>\&opcion_horasextras_periodos},
	]},
	{titulo=>'Bandeja de Viáticos', items=> [
		{opcion=>'consultaVI', titulo=>'Consulta', icono=>'fas fa-search', funcion=>\&opcion_viaticos_consulta},
		{opcion=>'resumenVI', titulo=>'Pendientes', icono=>'fas fa-circle', funcion=>\&opcion_viaticos_resumen},
		{opcion=>'erroresVI', titulo=>'Errores', icono=>'fas fa-times-circle', funcion=>\&opcion_viaticos_errores},
		{opcion=>'periodosVI', titulo=>'Períodos', icono=>'far fa-calendar-alt', funcion=>\&opcion_viaticos_periodos},
	]},
	{titulo=>'Bandeja de Multas', items=> [
		{opcion=>'consultaMU', titulo=>'Consulta', icono=>'fas fa-search', funcion=>\&opcion_multas_consulta},
		{opcion=>'resumenMU', titulo=>'Pendientes', icono=>'fas fa-circle', funcion=>\&opcion_multas_resumen},
		{opcion=>'erroresMU', titulo=>'Errores', icono=>'fas fa-times-circle', funcion=>\&opcion_multas_errores},
		{opcion=>'periodosMU', titulo=>'Períodos', icono=>'far fa-calendar-alt', funcion=>\&opcion_multas_periodos},
	]},
	{titulo=>'SIAP', items=> [
		{opcion=>'siap', titulo=>'Consulta', icono=>'fas fa-search', funcion=>\&opcion_siap_consulta},
		{opcion=>'procesos', titulo=>'Procesos', icono=>'fas fa-tasks', funcion=>\&opcion_siap_procesos},
	]},
	{items=> [
		{opcion=>'cargar', funcion=>\&ajax_cargar},
	]},
);

my %param = (
	dbh_siap => $dbh_siap,
	dbh_personal => $dbh_personal,
	bandeja => $bandeja,
	cedula => (defined($tvars{nombre}) ? $cedula : undef),
);

my $err;

if ($opcion) {
	# Busco la configuración de la opción que vino por parámetro
	my $opc;
	foreach my $dir (@opciones) {
		foreach my $item (@{$dir->{items}}) {
			if ($item->{opcion} eq $opcion) {
				$opc = $item;
				last;
			}
		}
	}
	if (defined($opc) && defined($opc->{funcion})) {
		$err = &{$opc->{funcion}}(\%param, \%tvars);
	}
}

$tvars{opciones} = \@opciones;

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

        my $js = "    body = [\n";
        foreach my $row (@{$data->{data}}) {
                $js .= '  ["'.join('","',@$row).'"],'."\n";
        }
	$js =~ s/,\n$/\n/s;
	$js .= "];\n";
        $js .= '    head = [ {title: "'.join('"},{title: "',@{$data->{head}}).'"} ];'."\n";

	return $js;
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

		next if ($_->[10] =~ /Baja lógica/);

		# Encontré una designación que está en fecha
		my $obs = $_->[$colobs];
		my $reserva = 0;

		while(!$reserva && $obs && $obs =~ s/(?:CORRIMIENTO|RESERVA DE CARGO|Licencia sin sueldo|Toma Cargo de Mayor Jerarquía)[^\d]*: (\d\d\d\d-\d\d-\d\d) a (\d\d\d\d-\d\d-\d\d)//i) {
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
		my $corp = corporativo::buscar($dbh_personal, $cedula);

		# agrego las horas de coordinación
		#corporativo::coordinacion($dbh_personal, $cedula, $corp->{data});

		$rtvars->{js} = data2js($corp);
		if ($rtvars->{js}) {
			$rtvars->{subtitulo} = "Designaciones:";

			$rtvars->{resumen_posesiones} = resumen_posesiones($corp);

			$rtvars->{horas_por_periodo} = corporativo::horas_por_periodo($dbh_personal, $cedula);
		}
		$rtvars->{hay_resultado} = 1;
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
		my $corp = corporativo::certificados($dbh_personal, $cedula);

		$rtvars->{js} = data2js($corp);
		$rtvars->{subtitulo} = "Certificados Médicos:";
		$rtvars->{hay_resultado} = 1;
	}

	$rtvars->{titulo} = "Consulta de certificados médicos en el Sistema Corporativo";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_bandejadd_consulta($$) {
	my ($rparam, $rtvars) = @_;

	my $cedula = $rparam->{cedula};
	my $dbh_siap = $rparam->{dbh_siap};

	if (defined($cedula)) {
		my $resultado = bandeja_dd::buscar($dbh_siap, $cedula);

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
			my $rowid = 0;
			foreach my $heading (@{$resultado->{head}}) {
				last if ($heading eq "Mensaje");
				$rowid++;
			}
			foreach my $row (@{$resultado->{data}}) {
				$row->[$rowid] =~ /Pendiente/ and $pendientes = 1 and last;
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
		$rtvars->{hay_resultado} = 1;
	}

	$rtvars->{titulo} = "Consulta a la bandeja de docencia directa";
	$rtvars->{buscador_cedulas} = 1;
	$rtvars->{bandeja} = "dd";

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

sub opcion_bandejadd_resumen($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $resumen = bandeja_dd::resumen($dbh_siap);

	$rtvars->{js} = data2js($resumen);
	$rtvars->{titulo} = "Datos pendientes en la bandeja DD";
	$rtvars->{hay_resultado} = 1;

	if ($rtvars->{admin}) {
		$rtvars->{btn} = 'Cargar bandeja';
		$rtvars->{btn_class} = 'btn-primary';
		$rtvars->{modal_opcion} = 'cargar';
		$rtvars->{bandeja} = 'dd';
		$rtvars->{cedula} = 'Docencia Directa';
	}

	return 0;
}

sub opcion_bandejadd_errores($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $errores = bandeja_dd::errores($dbh_siap);

	$rtvars->{js} = data2js($errores);
	$rtvars->{titulo} = "Personas con último pasaje en error";
	$rtvars->{hay_resultado} = 1;

	if ($#{$errores->{data}} > -1) {
		# Agrego enlaces en la primer columna, para acceder a la consulta de esa cédula
		$rtvars->{js} .= "
\$('table#maintable').on( 'draw.dt', function () {
  \$('table#maintable tbody tr td:nth-child(2)').addClass('link');
  \$('table#maintable tbody tr td:nth-child(2)').click(function(){
      window.location.href = '?opcion=consultaDD&cedula='+\$(this).text();
  });
});

		";
		$rtvars->{data_table_options} = '
			"order": [[ 0, "desc" ]]
		';
	}

	return 0;
}

sub opcion_bandejadd_periodos($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $periodos = periodos->new("dd",$dbh_siap);

	my $listado = $periodos->listado();

	$rtvars->{js} = data2js($listado);
	$rtvars->{titulo} = "Períodos de liquidación";
	$rtvars->{hay_resultado} = 1;

	return 0;
}

sub opcion_bandejadi_consulta($$) {
	my ($rparam, $rtvars) = @_;

	my $cedula = $rparam->{cedula};
	my $dbh_siap = $rparam->{dbh_siap};

	if (defined($cedula)) {
		my $resultado = bandeja_di::buscar($dbh_siap, $cedula);

		(defined($resultado)) || error($DBI::errstr,200);

		if ($#{$resultado->{data}} >= 0) {

			$rtvars->{js} = data2js($resultado);
			$rtvars->{subtitulo} = "Últimos datos en la bandeja:";

			# esto va para afuera del if:
			$rtvars->{resumen_posesiones} = resumen_posesiones($resultado);
		}
		$rtvars->{hay_resultado} = 1;
	}

	$rtvars->{titulo} = "Consulta a la bandeja de docencia indirecta";
	$rtvars->{buscador_cedulas} = 1;
	$rtvars->{bandeja} = "di";

	return 0;
}

sub opcion_bandejadi_resumen($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $resumen = bandeja_di::resumen($dbh_siap);

	$rtvars->{js} = data2js($resumen);
	$rtvars->{titulo} = "Datos pendientes en la bandeja DI";
	$rtvars->{hay_resultado} = 1;

	if ($rtvars->{admin}) {
		$rtvars->{btn} = 'Cargar bandeja';
		$rtvars->{btn_class} = 'btn-primary';
		$rtvars->{modal_opcion} = 'cargar';
		$rtvars->{bandeja} = 'di';
		$rtvars->{cedula} = 'Docencia Indirecta';
	}

	return 0;
}

sub opcion_bandejadi_errores($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $errores = bandeja_di::errores($dbh_siap);

	$rtvars->{js} = data2js($errores);
	$rtvars->{titulo} = "Personas con último pasaje en error";
	$rtvars->{hay_resultado} = 1;

	if ($#{$errores->{data}} > -1) {
		# Agrego enlaces en la primer columna, para acceder a la consulta de esa cédula
		$rtvars->{js} .= "
\$('table#maintable').on( 'draw.dt', function () {
  \$('table#maintable tbody tr td:nth-child(2)').addClass('link');
  \$('table#maintable tbody tr td:nth-child(2)').click(function(){
      window.location.href = '?opcion=consultaDI&cedula='+\$(this).text();
  });
});

		";
		$rtvars->{data_table_options} = '
			"order": [[ 0, "desc" ]]
		';
	}

	return 0;
}

sub opcion_bandejadi_periodos($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $periodos = periodos->new("di",$dbh_siap);

	my $listado = $periodos->listado();

	$rtvars->{js} = data2js($listado);
	$rtvars->{titulo} = "Períodos de liquidación";
	$rtvars->{hay_resultado} = 1;

	return 0;
}

sub opcion_horasextras_consulta($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $cedula = $rparam->{cedula};

	if (defined($cedula)) {

		my $multas = horasextras::buscar($dbh_siap,$cedula);

		$rtvars->{js} = data2js($multas);
		$rtvars->{subtitulo} = "Datos del año actual en la bandeja para esta cédula";
		$rtvars->{hay_resultado} = 1;
	}

	$rtvars->{titulo} = "Bandeja de Horas Extras";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_horasextras_resumen($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $resumen = horasextras::resumen($dbh_siap);

	$rtvars->{js} = data2js($resumen);
	$rtvars->{titulo} = "Datos pendientes en la bandeja de Horas Extras";
	$rtvars->{hay_resultado} = 1;

	if ($rtvars->{admin}) {
		$rtvars->{btn} = 'Cargar bandeja';
		$rtvars->{btn_class} = 'btn-primary';
		$rtvars->{modal_opcion} = 'cargar';
		$rtvars->{bandeja} = 'he';
		$rtvars->{cedula} = 'Horas Extras';
	}

	return 0;
}

sub opcion_horasextras_errores($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $errores = horasextras::errores($dbh_siap);

	$rtvars->{js} = data2js($errores);
	$rtvars->{titulo} = "Personas con último pasaje en error";
	$rtvars->{hay_resultado} = 1;

	if ($#{$errores->{data}} > -1) {
		# Agrego enlaces en la primer columna, para acceder a la consulta de esa cédula
		$rtvars->{js} .= "
\$('table#maintable').on( 'draw.dt', function () {
  \$('table#maintable tbody tr td:nth-child(2)').addClass('link');
  \$('table#maintable tbody tr td:nth-child(2)').click(function(){
      window.location.href = '?opcion=consultaHE&cedula='+\$(this).text();
  });
});

		";
		$rtvars->{data_table_options} = '
			"order": [[ 0, "desc" ]]
		';
	}

	return 0;
}

sub opcion_horasextras_periodos($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $periodos = periodos->new("he",$dbh_siap);

	my $listado = $periodos->listado();

	$rtvars->{js} = data2js($listado);
	$rtvars->{titulo} = "Períodos de liquidación";
	$rtvars->{hay_resultado} = 1;

	return 0;
}

sub opcion_viaticos_consulta($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $cedula = $rparam->{cedula};

	if (defined($cedula)) {

		my $multas = viaticos::buscar($dbh_siap,$cedula);

		$rtvars->{js} = data2js($multas);
		$rtvars->{subtitulo} = "Datos del año actual en la bandeja para esta cédula";
		$rtvars->{hay_resultado} = 1;
	}

	$rtvars->{titulo} = "Bandeja de Viáticos";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_viaticos_resumen($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $resumen = viaticos::resumen($dbh_siap);

	$rtvars->{js} = data2js($resumen);
	$rtvars->{titulo} = "Datos pendientes en la bandeja de Viáticos";
	$rtvars->{hay_resultado} = 1;

	if ($rtvars->{admin}) {
		$rtvars->{btn} = 'Cargar bandeja';
		$rtvars->{btn_class} = 'btn-primary';
		$rtvars->{modal_opcion} = 'cargar';
		$rtvars->{bandeja} = 'vi';
		$rtvars->{cedula} = 'Viáticos';
	}

	return 0;
}

sub opcion_viaticos_errores($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $errores = viaticos::errores($dbh_siap);

	$rtvars->{js} = data2js($errores);
	$rtvars->{titulo} = "Personas con último pasaje en error";
	$rtvars->{hay_resultado} = 1;

	if ($#{$errores->{data}} > -1) {
		# Agrego enlaces en la primer columna, para acceder a la consulta de esa cédula
		$rtvars->{js} .= "
\$('table#maintable').on( 'draw.dt', function () {
  \$('table#maintable tbody tr td:nth-child(2)').addClass('link');
  \$('table#maintable tbody tr td:nth-child(2)').click(function(){
      window.location.href = '?opcion=consultaHE&cedula='+\$(this).text();
  });
});
		";
		$rtvars->{data_table_options} = '
			"order": [[ 0, "desc" ]]
		';
	}

	return 0;
}

sub opcion_viaticos_periodos($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $periodos = periodos->new("vi",$dbh_siap);

	my $listado = $periodos->listado();

	$rtvars->{js} = data2js($listado);
	$rtvars->{titulo} = "Períodos de liquidación";
	$rtvars->{hay_resultado} = 1;

	return 0;
}

sub opcion_multas_consulta($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $cedula = $rparam->{cedula};

	if (defined($cedula)) {

		my $multas = multas::buscar($dbh_siap,$cedula);

		$rtvars->{js} = data2js($multas);
		$rtvars->{subtitulo} = "Datos del año actual en la bandeja para esta cédula";
		$rtvars->{hay_resultado} = 1;
	}

	$rtvars->{titulo} = "Bandeja de multas";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_multas_resumen($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $resumen = multas::resumen($dbh_siap);

	$rtvars->{js} = data2js($resumen);
	$rtvars->{titulo} = "Datos pendientes en la bandeja de Multas";
	$rtvars->{hay_resultado} = 1;

	if ($rtvars->{admin}) {
		$rtvars->{btn} = 'Cargar bandeja';
		$rtvars->{btn_class} = 'btn-primary';
		$rtvars->{modal_opcion} = 'cargar';
		$rtvars->{bandeja} = 'he';
		$rtvars->{cedula} = 'Multas';
	}

	return 0;
}

sub opcion_multas_errores($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $errores = multas::errores($dbh_siap);

	$rtvars->{js} = data2js($errores);
	$rtvars->{titulo} = "Personas con último pasaje en error";
	$rtvars->{hay_resultado} = 1;

	if ($#{$errores->{data}} > -1) {
		# Agrego enlaces en la primer columna, para acceder a la consulta de esa cédula
		$rtvars->{js} .= "
\$('table#maintable').on( 'draw.dt', function () {
  \$('table#maintable tbody tr td:nth-child(2)').addClass('link');
  \$('table#maintable tbody tr td:nth-child(2)').click(function(){
      window.location.href = '?opcion=consultaMU&cedula='+\$(this).text();
  });
});

		";
		$rtvars->{data_table_options} = '
			"order": [[ 0, "desc" ]]
		';
	}

	return 0;
}

sub opcion_multas_periodos($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $periodos = periodos->new("mu",$dbh_siap);

	my $listado = $periodos->listado();

	$rtvars->{js} = data2js($listado);
	$rtvars->{titulo} = "Períodos de liquidación";
	$rtvars->{hay_resultado} = 1;

	return 0;
}


sub opcion_siap_consulta($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $cedula = $rparam->{cedula};

	if (defined($cedula)) {

		my $siap = siap::buscar($dbh_siap, $cedula);

		$rtvars->{js} = data2js($siap);
		$rtvars->{subtitulo} = "Últimos datos en SIAP";

		$rtvars->{resumen_posesiones} = resumen_posesiones($siap);
		$rtvars->{hay_resultado} = 1;
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
		my $suspensiones = siap::suspensiones($dbh_siap, $cedula);

		$rtvars->{js} = data2js($suspensiones);
		$rtvars->{subtitulo} = "Suspensiones:";
		$rtvars->{hay_resultado} = 1;
	}

	$rtvars->{titulo} = "Consulta de marcas de suspensión de pagos en SIAP";
	$rtvars->{buscador_cedulas} = 1;

	return 0;
}

sub opcion_siap_procesos($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};

	my $procesos = siap::procesos($dbh_siap);

	$rtvars->{js} = data2js($procesos);
	$rtvars->{titulo} = "Procesos activos en SIAP";
	$rtvars->{hay_resultado} = 1;

	return 0;
}

sub ajax_borrar($$) {
	my ($rparam, $rtvars) = @_;

	my $bandeja = $rparam->{bandeja};
	my $cedula = $rparam->{cedula};

	if (!$cedula) {
		json_response({error=>"Parámetros incorrectos"});
	}

	my ($code, $text) = proxy("http://ssueldos01.ces.edu.uy/cgi-bin/sueldos/borrar.pl?bandeja=$bandeja\&cedula=$cedula");
	if ($code == 200) {
		json_response($text);
	} else {
		json_response({error=>"$text",code=>$code});
	}
	return 1;
}

sub ajax_reliquidar($$) {
	my ($rparam, $rtvars) = @_;

	my $dbh_siap = $rparam->{dbh_siap};
	my $bandeja = $rparam->{bandeja};
	my $cedula = $rparam->{cedula};

	my $periodos = periodos->new($bandeja,$dbh_siap);
	my ($desde,$hasta) = $periodos->minmax();

	if (!$desde || !$hasta) {
		json_response({error=>"No se puede obtener el período de reliquidación desde la base de datos: ".$DBI::errstr});
		return 1;
	}
	if (!$cedula) {
		json_response({error=>"Parámetros incorrectos"});
	}

	my ($code, $text) = proxy("http://ssueldos01.ces.edu.uy/cgi-bin/sueldos/reliquidar.pl?bandeja=$bandeja\&desde=$desde\&hasta=$hasta\&cedula=$cedula");
	if ($code == 200) {
		json_response($text);
	} else {
		json_response({error=>"$text",code=>$code});
	}
	return 1;
}

sub ajax_cargar($$) {
	my ($rparam, $rtvars) = @_;

	my $bandeja = $rparam->{bandeja};

	my ($code, $text) = proxy("http://ssueldos01.ces.edu.uy/cgi-bin/sueldos/reliquidar.pl?bandeja=$bandeja\&nuevo_periodo=on");
	if ($code == 200) {
		json_response($text);
	} else {
		json_response({error=>"$text",code=>$code});
	}
	return 1;
}
