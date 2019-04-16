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


chdir '/var/www/bandeja';

my $script_bandeja_dd = "reliquidar_dd.sh";
my $script_bandeja_di = "reliquidar_di.sh";
my $script_bandeja_he = "reliquidar_he.sh";
my $script_bandeja_vi = "reliquidar_vi.sh";

print header(-charset=>'utf-8',-type=>'application/json');

open(LOCKFH,$script_bandeja_dd);
if (! flock(LOCKFH, LOCK_EX|LOCK_NB)) {
	respuesta("Ya hay un proceso de reliquidación activo. Reintente luego");
	exit(0);
}
$SIG{INT} = sub { flock(LOCKFH, LOCK_UN); }; # libero el lock al salir

my $nuevo_periodo = checkFormat(param('nuevo_periodo'),'on');
my $cedula = checkFormat(param('cedula'),'\d\d\d\d\d\d\d\d');
my $bandeja = checkFormat(param('bandeja'),'(dd|di|he|vi)');

if (!$bandeja) {
	respuesta("No se recibió el parámetro 'bandeja'");
	exit(0);
}

my $dbh_tray = dbConnect("siap_ces_tray");

my $periodos = periodos->new($bandeja, $dbh_tray);

if ($nuevo_periodo) {
	if ($bandeja eq "dd" || $bandeja eq "he" || $bandeja eq "vi") {
		my ($desde,$hasta,$dias) = $periodos->ultimo();
		if (!defined($dias)) {
			respuesta("No se pudo obtener el período anterior");
			exit(0);
		}
		if ($dias<5) {
			respuesta("No se puede crear un nuevo período para la bandeja $bandeja porque el período anterior terminó hace menos de 5 días");
			exit(0);
		}
		my $out = $periodos->agregar_hasta_ayer();
		if ($out) {
			respuesta("No se pudo crear un período nuevo", $out);
			exit(0);
		}
	} else {

		my $out = $periodos->agregar_hasta_minutos(5);
		if ($out) {
			respuesta("No se pudo crear un período nuevo", $out);
			exit(0);
		}
	}
}

my ($desde,$hasta) = $cedula ? $periodos->minmax() : $periodos->ultimo();

#$desde='2018-03-01';
#$hasta='2018-05-11';
#$cedula='38152988';

$desde =~ s/ 00:00:00$//;
$hasta =~ s/ 00:00:00$//;

if ($bandeja eq "dd") {
	open(CMD, "/bin/bash $script_bandeja_dd --inicio '$desde' --fin '$hasta' ".($cedula ? "--ci '$cedula'" :"")." 2>&1 |");

} elsif ($bandeja eq "di") {
	open(CMD, "/bin/bash $script_bandeja_di --inicio '$desde' --fin '$hasta' 2>&1 |");
} elsif ($bandeja eq "he") {
	open(CMD, "/bin/bash $script_bandeja_he --inicio '$desde' --fin '$hasta' 2>&1 |");
} elsif ($bandeja eq "vi") {
	open(CMD, "/bin/bash $script_bandeja_vi --inicio '$desde' --fin '$hasta' 2>&1 |");
}
local $/ = undef;
my $out = <CMD>;
close(CMD);

if ($? ne 0) {
	respuesta("El proceso de reliquidación terminó con error ".$?, $out);
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
