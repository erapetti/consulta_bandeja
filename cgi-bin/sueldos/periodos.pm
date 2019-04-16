#!/usr/bin/perl
#
# periodos.pm
#
#	Períodos de liquidación de las bandejas

use strict;

package periodos;

sub new($$) {
	my $class = shift;
	my ($bandeja, $dbh) = @_;

	my %validos = ("dd"=>1, "di"=>1, "he"=>1, "vi"=>1);

	return undef if (!defined($validos{$bandeja}));

	my $self = {bandeja=>$bandeja, dbh=>$dbh};

	return bless $self, $class;
}

sub agregar_hasta_ayer() {
	my $this = shift;
	my $out;

	$this->{dbh}->do("
insert into periodos (id,bandeja,desde,hasta)
values (null,'".$this->{bandeja}."',(select * from (select max(hasta) from periodos where bandeja='".$this->{bandeja}."')X),curdate())
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";

	return $out;
}

sub agregar_hasta_minutos($) {
	my $this = shift;
	my ($minutos) = @_;
	my $out;

	$this->{dbh}->do("
insert into periodos (id,bandeja,desde,hasta)
values (null,'".$this->{bandeja}."',(select * from (select max(hasta) from periodos where bandeja='".$this->{bandeja}."')X),timestampadd(minute,-$minutos,now()))
	");
	($DBI::errstr) and $out .= $DBI::errstr.";";

	return $out;
}

sub minmax() {
	my $this = shift;

	my $SQL = "

SELECT min(desde),max(hasta)
FROM periodos
WHERE bandeja='".$this->{bandeja}."'
--  AND desde >= concat(year(curdate())+if(month(curdate())>=3,0,-1),'-03-01')
--  AND hasta <  concat(year(curdate())+if(month(curdate())>=3,1,0),'-03-01')
  AND hasta >= concat(year(curdate())+if(month(curdate())>=3,0,-1),'-03-01')

";

	my $sth = $this->{dbh}->prepare($SQL);
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my @row = $sth->fetchrow_array;
	$sth->finish;

	return ($row[0], $row[1]);
}

sub ultimo() {
	my $this = shift;

	my $SQL = "

SELECT p1.desde desde,p1.hasta hasta,DATEDIFF(curdate(),p1.hasta) dias
FROM periodos p1
LEFT JOIN periodos p2
on p1.id < p2.id
and p1.bandeja = p2.bandeja
WHERE p1.bandeja='".$this->{bandeja}."'
  AND p2.id is null

";

	my $sth = $this->{dbh}->prepare($SQL);
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my @row = $sth->fetchrow_array;
	$sth->finish;

	return ($row[0], $row[1], $row[2]);
}

sub listado() {
	my $this = shift;

	my $SQL = "

SELECT replace(desde,' 00:00:00','') desde,replace(hasta,' 00:00:00','')hasta
FROM siap_ces_tray.periodos
WHERE bandeja='".$this->{bandeja}."'
  AND (desde>='2019-03-01'
       OR hasta>='2019-03-01'
  )
ORDER BY id

";

	my $sth = $this->{dbh}->prepare($SQL);
	$sth->execute();

	(defined($sth) && !$DBI::errstr) or return undef;

	my $rows = $sth->fetchall_arrayref;

	$sth->finish;

	return {head=>["Desde","Hasta"], data=>$rows};
}

1;
