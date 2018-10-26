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

my $bandeja = "bj-8.0.9.py";

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


	# parche para arreglar la reserva de horas de Paola Trucco
	$dbh->do("
update ihorasclase
set horclainscod='25.3.0.0.8902'
where perdocnum='20260036'
  and DesFchProc is null
  and desfchcarga=curdate()
  and HorClaCic=3
  and ifnull(HorClaBajLog,0)=0
  and (HorClaAsiCod,HorClaHor) in ((75,3),(14,4))
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";

	# parche para arreglar la reserva de horas de Melisa Pastorini
	$dbh->do("
update ihorasclase
set HorClaFchPos='2018-10-11'
where perdocnum='40554843'
  and DesFchProc is null
  and desfchcarga=curdate()
  and horclainscod='25.3.3101.10.1029'
  and ifnull(HorClaBajLog,0)=0
  and HorClaFchPos='2018-03-01'
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";


if (0) {
	# borro registros de docentes PROCES hasta nuevo aviso
	$dbh->do("
delete from ihorasclase
where DesFchProc is null
  and DesFchCarga = curdate()
  and perdocnum in ('11424902','11483887','11863516','12497752','12668030','12932243','13733216','14272710','14387599','14957655','15196761','15271030','15641089','15857507','15872086','15993630','16080343','16144559','16556194','16988917','17083926','17090905','17183069','17347465','17623118','17749201','18005705','18113724','18310196','18377841','18678493','18788175','18979590','18991538','18996754','19023895','19077692','19092256','19272163','19332191','19545184','19569459','19596434','19782360','19910989','20012041','20054643','20114869','20127416','20260036','25285128','26103852','26206527','26320270','26990455','27025269','27404079','27522182','27672775','27675482','27695535','28324319','28391562','28433508','28886070','29272428','29371222','29560906','30042468','30118556','30322606','30464896','30500424','30503216','30550087','30560143','30579738','30693293','30738457','30746703','30781539','30927244','31058397','31334692','31389302','31501667','31733652','31910432','32157722','32340731','32462884','32592324','32863927','32949369','33050074','33354054','33491379','33648279','33728128','33803710','34121812','34480646','34531421','34574613','34575203','34590055','34646391','34690336','35047300','35095090','35585213','35803085','36013447','36222553','36432403','36497699','36564355','36675683','36725232','36747907','36868686','37585710','37626279','37846893','37922122','38232774','38381448','38470368','38928296','39072892','39161477','39303095','39303716','39307154','39398183','39977709','40047870','40070122','40261917','40392768','40402086','40529521','40833457','40932502','41140433','41240946','41258656','41292698','41526956','41698076','41753753','41893581','41904201','41945702','42217316','42225446','42322515','42328804','42387416','42445565','42507723','42541163','42764074','42773584','42887789','42900331','43075410','43111991','43200009','43208487','43378361','43381811','43449829','43480370','43505431','43507655','43723029','44018669','44116130','44140395','44202365','44280521','44351960','44459378','44488709','44525381','44545020','44929266','45089085','45259115','45364405','45366265','45382817','45932999','45938404','46109153','46332067','46361915','46463705','46509935','46716526','46838992','47016921','47146217','47238208','47699474','47750707','47832024','48067670','48096421','48159003','48212516','48343113','48463901','48526470','48936352','49287756','49324479','49384944','49395446','49450496','49839375','50277194','50480331','50757110','50945141','51493503','51658408')
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";
}

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
