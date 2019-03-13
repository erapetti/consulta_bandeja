#!/usr/bin/perl
#
# Genera la reliquidacion de un docente por bandeja de docencia directa

use strict;
use DBI;
use CGI qw/:standard/;
use Fcntl qw/:flock/;
use portal3 qw/checkFormat/;

sub dbConnect(;$$$) ;
sub posbandeja($$) ;
sub nuevo_periodo($) ;
sub periodo_minmax($) ;
sub ultimo_periodo($) ;
sub nuevo_periodo_dias($) ;
sub respuesta($;$) ;


chdir '/var/www/bandeja';

my $bandeja = "bj-8.0.13.py";

open(LOCKFH,$bandeja);
if (! flock(LOCKFH, LOCK_EX|LOCK_NB)) {
	respuesta("Ya hay un proceso de reliquidación activo. Reintente luego");
	exit(0);
}

print header(-charset=>'utf-8',-type=>'application/json');

my $nuevo_periodo = checkFormat(param('nuevo_periodo'),'on');
my $cedula = checkFormat(param('cedula'),'\d\d\d\d\d\d\d\d');

my $dbh_tray = dbConnect("siap_ces_tray");

if ($nuevo_periodo) {
	my $dias = nuevo_periodo_dias($dbh_tray);
	if (!$dias or $dias<5) {
		respuesta("No se puede crear un nuevo período porque el período anterior terminó hace menos de 5 días");
		exit(0);
	}
	my $out = nuevo_periodo($dbh_tray);
	if ($out) {
		flock(LOCKFH, LOCK_UN);
		respuesta("No se pudo crear un período nuevo", $out);
		exit(0);
	}
}


my ($desde,$hasta) = ($nuevo_periodo ? ultimo_periodo($dbh_tray) : periodo_minmax($dbh_tray) );

#$desde='2018-03-01';
#$hasta='2018-05-11';
#$cedula='38152988';

open(CMD, "/usr/bin/python $bandeja --inicio $desde --fin $hasta ".($cedula ? "--ci $cedula" :"")." 2>&1 |");
local $/ = undef;
my $out = <CMD>;
close(CMD);

if ($? ne 0) {
	respuesta("El proceso de reliquidación terminó con error ".$?, $out);
	exit(0);
}

my $err = posbandeja($dbh_tray,$cedula);
if ($err) {
	respuesta("El proceso de posbandeja terminó con error: ".$?, $out);
	exit(0);
}

respuesta("", $out);
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
sub posbandeja($$) {
	my ($dbh,$cedula) = @_;
	my $out;

	# Borro registros posteriores a hoy
	$dbh->do("
delete ihc
from ihorasclase ihc
join (select max(hasta) hasta from periodos) P
where DesFchProc is null
  and DesFchCarga = curdate()
  and (HorClaFchCese < HorClaFchPos or horclafchpos>=P.hasta)
--  and not(HorClaBajLog=1 and HorClaCauBajCod=99)
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
where (HC1.HorClaBajLog=0 or HC1.HorClaBajLog is null)
  and HC1.Resultado='OK'
  and HC1.DesFchCarga>='2018-03-01'
  and HC2.perdocnum is null
".($cedula ? "  and HC1.perdocnum = '$cedula'" : "  and HC1.perdocnum in (select perdocnum from ihorasclase where desfchcarga=curdate() and DesFchProc is null and (HorClaBajLog=0 or HorClaBajLog is null) group by 1)")."
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


	# borro registros de docentes PROCES ANEP hasta nuevo aviso
	$dbh->do("
delete from ihorasclase
where DesFchProc is null
  and DesFchCarga = curdate()
  and horclainscod="25.1.0.0.8007";
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";

	# parche para cambiar la materia 90 (Coordinación) del Corporativo por la 75 de SIAP
	$dbh->do("
update ihorasclase
set HorClaAsiCod=75
where HorClaAsiCod=90
  and DesFchProc is null
  and desfchcarga = curdate()
".($cedula ? "  and perdocnum = '$cedula'" : "")."
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";


	$dbh->disconnect();
	return $out;
}

sub nuevo_periodo ($) {
	my ($dbh) = @_;
	my $out;

	$dbh->do("
insert into periodos
values (null,(select * from (select max(hasta) from periodos)X),curdate())
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";

	$dbh->disconnect();
	return $out;
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

sub ultimo_periodo($) {
	my ($dbh) = @_;

	my $SQL = "

SELECT p1.desde,p1.hasta
FROM periodos p1
LEFT JOIN periodos p2
on p1.id < p2.id
WHERE p2.id is null

";

	my $sth = $dbh->prepare($SQL);
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my @row = $sth->fetchrow_array;
	$sth->finish;

	return ($row[0], $row[1]);
}

sub nuevo_periodo_dias($) {
	my ($dbh) = @_;

	my $SQL = "

SELECT DATEDIFF(curdate(),(
    select max(hasta) from periodos
));

";

	my $sth = $dbh->prepare($SQL);
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my @row = $sth->fetchrow_array;
	$sth->finish;

	return $row[0];
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
