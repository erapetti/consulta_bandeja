#!/usr/bin/perl
#
# Genera la reliquidacion de un docente por bandeja de docencia directa

use strict;
use DBI;
use CGI qw/:standard/;
use Fcntl qw/:flock/;

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

open(LOCKFH,"bj-7.3.5.py");
if (! flock(LOCKFH, LOCK_EX|LOCK_NB)) {
	print '{"error":"Ya hay un proceso de reliquidación activo. Reintente luego"}';
	exit(0);
}

open(CMD, "/usr/bin/python bj-7.3.5.py --inicio $desde --fin $hasta --ci $cedula |");
local $/ = undef;
my $out = <CMD>;
close(CMD);

if ($? ne 0) {
	print '{"error":"El proceso de reliquidación terminó con error '.$?.'","salida":"'.$out.'"}';
	exit(0);
}

my $out =~ s/\n/ /g;
$out =~ s/"/\\"/g;

$out .= posbandeja($cedula);

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

# valida el string contra una regexp
sub checkFormat($$) {
        my ($str,$fmt) = @_;

        if (defined($str)) {
                my $aux = $str;
                $aux =~ /^\s*($fmt)\s*$/ and return $1;
        }

        return undef;
}

# Correcciones de los pendientes en la bandeja
sub posbandeja($) {
	my ($cedula) = @_;
	my $out;

	my $dbh = dbConnect("siap_ces_tray");

	$dbh->do("
insert into ihorasclase
(PerDocTpo,PerDocPaisCod,PerDocNum,HorClaCarNumVer,HorClaFchIng,HorClaInsCod,HorClaCurTpo,HorClaCur,HorClaArea,HorClaAnio,HorClaGrupo,HorClaHorTope,HorClaCar,HorClaFchPos,HorClaFchCese,HorClaObs,HorClaNumInt,HorClaParPreCod,HorClaCompPor,HorClaLote,HorClaAudUsu,HorClaFchLib,HorClaCauBajCod,HorClaAsiCod,HorClaMod,HorClaHor,HorClaBajLog,HorClaCic,HorClaEmpCod,DesFchProc,Resultado,Mensaje,HorClaCarNum,DesFchCarga,NroLote)
select null HorClaId,
       HC1.PerDocTpo,HC1.PerDocPaisCod,HC1.PerDocNum,HC1.HorClaCarNumVer,HC1.HorClaFchIng,HC1.HorClaInsCod,HC1.HorClaCurTpo,HC1.HorClaCur,HC1.HorClaArea,HC1.HorClaAnio,HC1.HorClaGrupo,HC1.HorClaHorTope,HC1.HorClaCar,HC1.HorClaFchPos,HC1.HorClaFchCese,HC1.HorClaObs,HC1.HorClaNumInt,HC1.HorClaParPreCod,HC1.HorClaCompPor,HC1.HorClaLote,HC1.HorClaAudUsu,HC1.HorClaFchLib,HC1.HorClaCauBajCod,HC1.HorClaAsiCod,HC1.HorClaMod,HC1.HorClaHor,
       1 HorClaBajLog,
       HC1.HorClaCic,HC1.HorClaEmpCod,
       null DesFchProc,
       '' Resultado,
       '' Mensaje,
       HC1.HorClaCarNum,
       curdate() DesFchCarga,
       null NroLote
from ihorasclase HC1
left join ihorasclase HC2
  on HC1.perdocnum=HC2.perdocnum
 and HC1.desfchcarga<HC2.desfchcarga
 and HC1.horclainscod=HC2.horclainscod
 and HC1.Resultado=HC2.Resultado
 and HC1.HorClaBajLog=HC2.HorClaBajLog
where HC2.perdocnum is null
  and HC1.Resultado='OK'
  and HC1.HorClaBajLog=0
  and (HC1.perdocnum,HC1.horclainscod) in
  ( -- personas/liceos para arreglar: las que están en SIAP y no están en la bandeja
    select PerDocNum,InsCod
    from (
      select PerDocNum,InsCod,desfchcarga
      from ihorasclase
      join siap_ces.v_designaciones using (PerDocNum)
      where desfchproc is null
        and ( tipo='DD' or tipo='DI' and AsiCod=151 )
        and ( DesFchEgr>='2018-03-01' or DesFchEgr='1000-01-01' )
        and perdocnum='$cedula'
      group by 1,2,3
    )X
    left join (
      select PerDocNum,horclainscod InsCod,desfchcarga
      from ihorasclase
      where desfchproc is null and ( horclafchcese>='2018-03-01' or horclafchcese='1000-01-01' )
      group by 1,2,3
    )Y using (PerDocNum,InsCod,desfchcarga)
    where Y.perdocnum is null and inscod not in ('25.3.0.0.8901','25.3.0.0.8902')
  )
        ");

	($DBI::errstr) and $out .= $DBI::errstr."<br>\n";

	$dbh->disconnect();

}
