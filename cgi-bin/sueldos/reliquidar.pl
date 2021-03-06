#!/usr/bin/perl
#
# Genera la reliquidacion de un docente por bandeja de docencia directa

use strict;
use DBI;
use CGI qw/:standard/;
use Fcntl qw/:flock/;
use portal3 qw/checkFormat/;
use periodos;

sub dbConnect(;$$$) ;
sub respuesta($;$) ;
sub calcularHastaDia15($) ;
sub calcularHastaHaceMinutos($) ;


chdir '/var/www/bandeja';

my $script_bandeja_dd = "reliquidar_dd.sh";
my $script_bandeja_di = "reliquidar_di.sh";
my $script_bandeja_he = "reliquidar_he.sh";
my $script_bandeja_vi = "reliquidar_vi.sh";
my $script_bandeja_mu = "reliquidar_mu.sh";
my $script_bandeja_ia = "reliquidar_ia.sh";

print header(-charset=>'utf-8',-type=>'application/json'),"\n";

print STDERR "headers\n";


open(LOCKFH,$script_bandeja_dd);
if (! flock(LOCKFH, LOCK_EX|LOCK_NB)) {
	respuesta("Ya hay un proceso de reliquidación activo. Reintente luego");
	exit(0);
}
$SIG{INT} = sub { flock(LOCKFH, LOCK_UN); }; # libero el lock al salir

my $cedula = checkFormat(param('cedula'),'\d\d\d\d\d\d\d\d');
my $bandeja = checkFormat(param('bandeja'),'(dd|di|he|vi|mu|ia)');

if (!$bandeja) {
	respuesta("No se recibió el parámetro 'bandeja'");
	exit(0);
}

my $dbh_tray = dbConnect("siap_ces_tray");

my $periodos = periodos->new($bandeja, $dbh_tray);

my ($desde,$hasta);

if (!$cedula) {
	if ($bandeja eq "dd" || $bandeja eq "he" || $bandeja eq "vi" || $bandeja eq "mu" || $bandeja eq "ia") {
		my $dias;
		($desde,$hasta,$dias) = $periodos->ultimo();
		if (!defined($dias)) {
			respuesta("No se pudo obtener el período anterior");
			exit(0);
		}
#		if ($dias<5) {
#			respuesta("No se puede crear un nuevo período para la bandeja $bandeja porque el período anterior terminó hace menos de 5 días");
#			exit(0);
#		}
		if ($bandeja eq "dd" || $bandeja eq "he" || $bandeja eq "vi") {
			$desde = $hasta;
			$hasta = calcularHastaHoy($desde);
		} else {
			$desde = $hasta;
			$hasta = calcularHastaDia15($desde);
		}

		if (!$hasta) {
			respuesta("No se pudo definir una fecha final a partir de la fecha inicial $desde");
			exit(0);
		}
		if ($desde ge $hasta) {
			respuesta("No se puede definir el período a reliquidar porque quedaría vacío, desde=$desde hasta=$hasta");
			exit(0);
		}
	} elsif ($bandeja eq "di") {

		($desde,$hasta) = $periodos->ultimo();

		$desde = $hasta;
		$hasta = calcularHastaHaceMinutos(5);
	}
} else {
	($desde,$hasta) = $periodos->minmax();
}


#$desde='2018-03-01';
#$hasta='2018-05-11';
#$cedula='38152988';


if ($bandeja eq "dd") {
	$desde =~ s/ 00:00:00$//;
	$hasta =~ s/ 00:00:00$//;
	open(CMD, "/bin/bash $script_bandeja_dd --inicio '$desde' --fin '$hasta' ".($cedula ? "--ci '$cedula'" :"")." 2>&1 |");

} elsif ($bandeja eq "di") {
	open(CMD, "/bin/bash $script_bandeja_di --inicio '$desde' --fin '$hasta' 2>&1 |");
} elsif ($bandeja eq "he") {
	$desde =~ s/ 00:00:00$//;
	$hasta =~ s/ 00:00:00$//;
	open(CMD, "/bin/bash $script_bandeja_he --inicio '$desde' --fin '$hasta' 2>&1 |");
} elsif ($bandeja eq "vi") {
	$desde =~ s/ 00:00:00$//;
	$hasta =~ s/ 00:00:00$//;
	open(CMD, "/bin/bash $script_bandeja_vi --inicio '$desde' --fin '$hasta' 2>&1 |");
} elsif ($bandeja eq "mu") {
	$desde =~ s/ 00:00:00$//;
	$hasta =~ s/ 00:00:00$//;
	open(CMD, "/bin/bash $script_bandeja_mu --inicio '$desde' --fin '$hasta' 2>&1 |");
} elsif ($bandeja eq "ia") {
	$desde =~ s/ 00:00:00$//;
	$hasta =~ s/ 00:00:00$//;
	open(CMD, "/bin/bash $script_bandeja_ia --inicio '$desde' --fin '$hasta' 2>&1 |");
}

my $out;
while (<CMD>) {
	if (/^$/) {
		# para evitar gateway timeout
		print "\n";
		print STDERR "pasa\n";
	} else {
		$out .= $_;
	}
}
close(CMD);

if ($? ne 0) {
	respuesta("El proceso de reliquidación terminó con código de error ".($?/256), $out);
	exit(0);
}

respuesta("", $out);
exit(0);


######################################################################

# Conexión a la base de datos
sub dbConnect(;$$$) {
        my ($base,$user,$pass) = @_;

        my ($db,$host,$dbh);

        $db=$base;
        if ($db eq "Personal") {
                $host="sdb-reader2.ces.edu.uy.";
        } else {
                $host="sdb690-07.ces.edu.uy";
        }
        $user=($user || "consulta_bandeja");
        $pass=($pass || "sdf9d3klj3");

        $dbh = DBI->connect("DBI:mysql:$db:$host:3306",$user,$pass) || return undef;
        $dbh->do("set character set utf8");

        return $dbh;
}

sub respuesta($;$) {
	my ($error, $out) = @_;

	$error =~ s/\n/ /g;
	$error =~ s/"/\\"/g;
	if ($out) {
		$out =~ s/\n/ /g;
		$out =~ s/"/\\"/g;
	}
	print '{"error":"'.$error.($out ? '","salida":"'.$out : '').'"}';
}

sub calcularHastaHoy() {

	my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
	my $hasta = sprintf "%04d-%02d-%02d 00:00:00", $year+1900, $mon+1, $mday;

	return $hasta;
}

sub calcularHastaDia15($) {
	my ($desde) = @_;
	my $hasta;

	$desde =~ /^(\d\d\d\d)-(\d\d)-\d\d/ or return undef;

	my ($anio,$mes) = ($1,$2);

	# Calculo hasta como el día 15 del siguiente mes a desde:
	$mes++;
	if ($mes>12) {
		$mes=1;
		$anio++;
	}
	$hasta = sprintf "%04d-%02d-15 00:00:00", $anio, $mes;

	# Como tope hasta podría ser el dia de hoy:
	my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
	my $max = sprintf "%04d-%02d-%02d 00:00:00", $year+1900, $mon+1, $mday;

	return ($hasta gt $max ? $max : $hasta);
}

sub calcularHastaHaceMinutos($) {
	my ($minutos) = @_;

	my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time() - $minutos * 60);

	return sprintf "%04d-%02d-%02d %02d:%02d:%02d", $year+1900, $mon+1, $mday, $hour, $min, $sec;
}
