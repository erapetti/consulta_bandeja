use strict;

package siap;


sub buscar($$) {
	my ($dbh, $cedula) = @_;

	my $sth = $dbh->prepare("

SELECT perdocnum,
       DesFchIng,
       DesFchEgr,
       InsDsc,
       RelLabIds,
       ifnull(AsiNom,cargo),
       CarNum,
       R.reservas,
       CicCod,
       sum(horas)
FROM siap_ces.v_designaciones v
LEFT JOIN siap_ces.asignaturas using (AsiCod)
LEFT JOIN (
  SELECT dc.DesConEmpCod,
         dc.DesConCarNum,
         dc.DesConCarNumVer,
         group_concat(concat(ConDsc,': ',DesConFchDes,' a ',DesConFchHas) separator '<br>') reservas
  FROM siap_ces.designacionesconceptos dc
  JOIN siap_ces.conceptos using (ConCod)
--  WHERE dc.ConCod in (81150,81151,81154,81155,81156)
  WHERE dc.ConCod in (81111,81113,81115,81116,81150,81151,81154,81155,81156,81152,82063,82072,82070)
    AND (dc.DesConFchHas='1000-01-01' OR year(dc.DesConFchHas)>=year(curdate()))
  GROUP BY 1,2,3
) R ON R.DesConEmpCod = v.EmpCod
   AND R.DesConCarNum = v.CarNum
   AND R.DesConCarNumVer = v.CarNumVer
WHERE perdocnum='".$cedula."'
  AND (DesFchEgr='1000-01-01' OR DesFchEgr>='2019-03-01')
GROUP BY 1,2,3,4,5,7
ORDER BY 2,4,5,7,3;

	");
	$sth->execute();

	(defined($sth)) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Dependencia","RelLab","Cargo/Asignatura","CarNum","Observaciones","Ciclo","Horas"], data=>$rows};
}

sub suspensiones($$) {
	my ($dbh, $cedula) = @_;

	my $desde = "concat(year(curdate())+if(month(curdate())<3,-1,0),'-03-01')";

	my $SQL = "
SELECT PerDocNum,
       Desde,
       Hasta,
       Motivo
FROM siap_ces_tray.suspensiones
WHERE PerDocNum='$cedula'
--  AND desde>=$desde
";
	my $sth = $dbh->prepare($SQL);
	$sth->execute() or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Cédula","Desde","Hasta","Motivo"], data=>$rows};
}

sub procesos ($) {
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

1;
