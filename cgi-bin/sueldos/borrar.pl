#!/usr/bin/perl
#
# borrar.pl
# 	Borra una liquidación pendiente de la bandeja de horas de docencia directa
#



use strict;
use DBI;
use CGI qw/:standard/;

sub dbConnect(;$$$) ;
sub checkFormat($$) ;


print header(-charset=>'utf-8',-type=>'application/json');

my $cedula = checkFormat(param('cedula'),'\d\d\d\d\d\d\d\d');

if (!$cedula) {
        print '{"error":"parámetros incorrectos para borrar"}';
        exit(0);
}

my $dbh = dbConnect("siap_ces_tray");

if ($DBI::errstr) {
	print '{"error":"'.$DBI::errstr.'"}';
	exit(0);
}

$dbh->do("delete from ihorasclase where perdocnum=$cedula and desfchproc is null");

print '{"error":"'.$DBI::errstr.'"}';
exit(0);

######################################################################


sub dbConnect(;$$$) {
        my ($base,$user,$pass) = @_;

        my ($db,$host,$dbh);

        $db=$base;
	$host="sdb690-07.ces.edu.uy";
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

