#!/usr/bin/perl
#
# Genera la reliquidacion de un docente por bandeja de docencia directa

use strict;
use DBI;
use CGI qw/:standard/;
use Fcntl qw/:flock/;
use portal3 qw/checkFormat/;

sub checkFormat($$) ;
sub posbandeja($) ;

print header(-charset=>'utf-8',-type=>'application/json');

my $desde = checkFormat(param('desde'),'\d\d\d\d-\d\d-\d\d');
my $hasta = checkFormat(param('hasta'),'\d\d\d\d-\d\d-\d\d');
my $cedula = checkFormat(param('cedula'),'\d\d\d\d\d\d\d\d');

#$desde='2018-03-01';
#$hasta='2018-05-11';
#$cedula='38152988';

if (!$desde || !$hasta) {
	print '{"error":"parámetros incorrectos para reliquidar"}';
	exit(0);
}

chdir '/var/www/bandeja';

my $bandeja = "bj-8.0.9.py";

open(LOCKFH,$bandeja);
if (! flock(LOCKFH, LOCK_EX|LOCK_NB)) {
	print '{"error":"Ya hay un proceso de reliquidación activo. Reintente luego"}';
	exit(0);
}

open(CMD, "/usr/bin/python $bandeja --inicio $desde --fin $hasta ".($cedula ? "--ci $cedula" :"")." 2>&1 |");
local $/ = undef;
my $out = <CMD>;
close(CMD);

if ($? ne 0) {
	print '{"error":"El proceso de reliquidación terminó con error '.$?.'","salida":"'.$out.'"}';
	exit(0);
}

$out =~ s/\n/ /g;
$out =~ s/"/\\"/g;

my $err = posbandeja($cedula);
if ($err) {
	print '{"error":"El proceso de posbandeja terminó con error: '.$err.'","salida":"'.$out.'"}';
	exit(0);
}

print '{"error":"","salida":"'.$out.'"}';
flock(LOCKFH, LOCK_UN);
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

# Correcciones de los pendientes en la bandeja
sub posbandeja($) {
	my ($cedula) = @_;
	my $out;

	my $dbh = dbConnect("siap_ces_tray");

	# Borro registros posteriores a hoy
	$dbh->do("
delete ihc
from ihorasclase ihc
join (select max(hasta) hasta from periodos) P
where DesFchProc is null
  and DesFchCarga = curdate()
  and (HorClaFchCese < HorClaFchPos or horclafchpos>=P.hasta)
  and not(HorClaBajLog=1 and HorClaCauBajCod=99)
".($cedula ? "  and perdocnum = '$cedula'" : "")."
        ");
	($DBI::errstr) and $out .= $DBI::errstr.";";


	# Ingreso bajas en dependencias donde ya no tiene horas
	$dbh->do("
insert into ihorasclase
(HorClaId,PerDocTpo,PerDocPaisCod,PerDocNum,HorClaCarNumVer,HorClaFchIng,HorClaInsCod,HorClaCurTpo,HorClaCur,HorClaArea,HorClaAnio,HorClaGrupo,HorClaHorTope,HorClaCar,HorClaFchPos,HorClaFchCese,HorClaObs,HorClaNumInt,HorClaParPreCod,HorClaCompPor,HorClaLote,HorClaAudUsu,HorClaFchLib,HorClaCauBajCod,HorClaAsiCod,HorClaMod,HorClaHor,HorClaBajLog,HorClaCic,HorClaEmpCod,DesFchProc,Resultado,Mensaje,HorClaCarNum,DesFchCarga,NroLote)

select null HorClaId,
       HC1.PerDocTpo,HC1.PerDocPaisCod,HC1.PerDocNum,HC1.HorClaCarNumVer,HC1.HorClaFchIng,HC1.HorClaInsCod,HC1.HorClaCurTpo,HC1.HorClaCur,HC1.HorClaArea,HC1.HorClaAnio,HC1.HorClaGrupo,HC1.HorClaHorTope,HC1.HorClaCar,HC1.HorClaFchPos,HC1.HorClaFchCese,HC1.HorClaObs,HC1.HorClaNumInt,HC1.HorClaParPreCod,HC1.HorClaCompPor,HC1.HorClaLote,HC1.HorClaAudUsu,HC1.HorClaFchLib,
       99 HorClaCauBajCod,
       HC1.HorClaAsiCod,HC1.HorClaMod,HC1.HorClaHor,
       1 HorClaBajLog,
       HC1.HorClaCic,HC1.HorClaEmpCod,
       null DesFchProc,
       'PE' Resultado,
       '' Mensaje,
       HC1.HorClaCarNum,
       curdate() DesFchCarga,
       null NroLote
from ihorasclase HC1
left join ihorasclase HC2
  on HC2.perdocnum=HC1.perdocnum
 and HC2.desfchcarga=curdate()
 and HC2.horclainscod=HC1.horclainscod
 and HC2.DesFchProc is null
where HC1.HorClaBajLog=0
  and HC1.Resultado='OK'
  and HC1.DesFchCarga>='2018-03-01'
  and HC2.perdocnum is null
".($cedula ? "  and HC1.perdocnum = '$cedula'" : "")."
        ");
	($DBI::errstr) and $out .= $DBI::errstr.";";


	# parche para cambiar la materia 98 (AAM) del Corporativo por la 77 de SIAP
	$dbh->do("
update ihorasclase
set HorClaAsiCod=77
where HorClaAsiCod=98
  and DesFchProc is null
  and desfchcarga = curdate()
".($cedula ? "  and perdocnum = '$cedula'" : "")."
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";

	$dbh->disconnect();
	return $out;
}

sub nuevo_periodo {
	my $out;

	my $dbh = dbConnect("siap_ces_tray");

	$dbh->do("
insert into periodos
values (null,(select * from (select max(hasta) from periodos)X),curdate())
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";

	$dbh->disconnect();
	return $out;
}
