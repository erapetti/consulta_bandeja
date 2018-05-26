use CGI;
use SOAP::Lite;
use URI::Escape;

package portal3;

sub getSession($) ;
sub dbDisconnect ($) ;
sub dbGet ($$$$$) ;
sub dbInsert ($$%) ;
sub dbUpdate ($$$%) ;
sub _dbUpdate ($$$%) ;
sub dbDelete ($$%) ;
sub dbAudit ($$$) ;
sub validoSesion($$$) ;
sub leoPermiso($$) ;
sub myScriptName() ;
sub checkFormat($$) ;


require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(
	getSession
	leoPermiso
	dbDisconnect 
	dbGet 
	dbInsert 
	dbUpdate 
	dbDelete 
	dbAudit 
	validoSesion
	myScriptName
	checkFormat
	error
);

$::URLPORTAL="/portal/cgi-bin/portal/login";

sub getSession($) {
         my ($cookie) = @_;

         $cookie =~ /^ *([a-zA-Z\d]+) *$/ || error("Sesión no válida. Reinicie su conexión con el portal de servicios",403);

         my $sessionid = $1;

         my $ws = SOAP::Lite->service('file:aws_dame_datos_de_sesion.wsdl')
                              ->proxy('http://servicios.ces.edu.uy/Portal/servlet/aws_dame_datos_de_sesion');

         my ($userid,$dependid,$lugarid) = $ws->call(SOAP::Data->name('Execute')->attr({xmlns => 'portal3Ev2'}),
                                                           SOAP::Data->name('Sesionesid')->value($sessionid))->paramsout;

         if (!defined($userid) || defined($userid->{faultcode})) {
                  error("Sesión no válida. Reinicie su conexión con el portal de servicios",403);
         }

         leoPermiso($sessionid,'DSP') || error("No tiene los privilegios requeridos para acceder a esta página",402);

         return ($userid,$sessionid,$dependid,$lugarid);
}

# Verifica si el usuario tiene el permiso dado (valores posibles: INS, DLT, EJE, UPD, DSP)
sub leoPermiso($$) {
        my ($sessionid,$modo) = @_;

        my $name = CGI::script_name();
        $name =~ s%.*/%%;
        if (CGI::param('deptoid')) {
		$name .= "?deptoid=".CGI::param('deptoid');
        }

	if (!defined($::__leoPermiso_ws)) {
		 $::__leoPermiso_ws = SOAP::Lite->service('file:aws_autorizar_usuario_objeto.wsdl')
				      ->proxy('http://servicios.ces.edu.uy/Portal/servlet/aws_autorizar_usuario_objeto');
	}

        my ($autorizado,$path) = $::__leoPermiso_ws->call(
					SOAP::Data->name('ws_autorizar_usuario_objeto.Execute')->attr({xmlns => 'portal3Ev2'}),
					(SOAP::Data->name('Sesionesid')->value($sessionid),
					 SOAP::Data->name('Programa')->value($name),
					 SOAP::Data->name('Modo')->value($modo)
					)
				)->paramsall;

#($autorizado eq 'S') || error("name = $name");

         return ($autorizado eq 'S');
}

sub dbDisconnect ($) {
	$_[0]->disconnect();
}

sub dbGet ($$$$$) {
	my ($dbh,$tabla,$rcolumnas,$condicion,$extra) = @_;

	my $sql;
	if ($rcolumnas && $tabla) {
		$sql = "SELECT ". join(',',@$rcolumnas). " FROM $tabla";
	}
	($condicion) and $sql .= " WHERE $condicion";
	($extra) and $sql .= " $extra";

	if ($::debug) {
		print CGI::escapeHTML("$sql;")."<br>\n";
	}

	my $sth = $dbh->prepare($sql);
	($::debug) && ($dbh->{'mysql_errno'}!=0) && print DBI::errstr."<br>\n";
	$sth->execute();

	return $sth;
}

sub dbInsert ($$%) {
	my ($dbh,$tabla,%valores) = @_;

	my $sql = "INSERT into $tabla (". join(',',keys %valores).
	      ") values ('". join("','",values %valores) ."')";
	$dbh->do($sql);

	return $dbh->{'mysql_errno'};
}


sub dbUpdate ($$$%) {
	my ($dbh,$tabla,$condicion,%valores) = @_;
	foreach $_ (keys %valores) {
		$valores{$_}="'".$valores{$_}."'";
	}

	return _dbUpdate($dbh,$tabla,$condicion,%valores);
}

sub _dbUpdate ($$$%) {
	my ($dbh,$tabla,$condicion,%valores) = @_;

	my $valores;
	foreach $_ (keys %valores) {
		$valores .= "$_=$valores{$_},";
	}
	$valores =~ s/,$//;
	my $sql = "UPDATE $tabla set $valores WHERE $condicion";
	$dbh->do($sql);

	return $dbh->{'mysql_errno'};
}

sub dbDelete ($$%) {
	my ($dbh,$tabla,%valores) = @_;

	my $where;
	foreach $_ (keys %valores) {
		$where.= " $_='$valores{$_}'";
	}
	return 0 if (!$where);

	my $sql = "DELETE from $tabla WHERE $where";

	$dbh->do($sql);

	return ! $dbh->{'mysql_errno'};
}

sub dbAudit ($$$) {
	my ($dbh,$usuario,$objeto) = @_;

	my $ip = $ENV{REMOTE_ADDR};
	if ($ip =~ /^10\.200\.0\./ && $ENV{HTTP_X_FORWARDED_FOR}) {
		$ip = $ENV{HTTP_X_FORWARDED_FOR};
		$ip =~ s/127\.0\.0\.1//;
		$ip =~ s/, $//;
		$ip =~ s/^, //;
	}

	my $programa = CGI::script_name();
	$programa =~ s%.*/%%;

	my $sql = sprintf("INSERT INTO audit (ip,usuario,objeto,programa) VALUES ('%s','%s','%s','%s')",$ip,$usuario,$objeto,$programa);
	$dbh->do($sql);

	return ! $dbh->{'mysql_errno'};
}

# Devuelve true si la sesión actual del usuario coincide con la dada
sub validoSesion($$$) {
	my ($userid,$sessionid,$dbh) = @_;

	my $sth = dbGet($dbh,"SEGUSUARIOS",["UserRnd"],"UserId='$userid'","");
	my @row = $sth->fetchrow_array;
	$sth->finish();
	
	($::debug) && print "En la base sessionid=$row[0] en la cookie sessionid=$sessionid\n";

	return (defined($row[0]) && $row[0] == $sessionid);
}

sub myScriptName() {
	my $script_name = CGI::script_name();
	$script_name =~ s%.*/%%;
	$script_name =~ s%\.pl$%%;

	return $script_name;
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

sub error($;$) {
	my ($texto,$codigo) = @_;

	my %param = (-charset=>'utf-8');
#	if ($codigo > 400) {
#		print CGI::redirect("$::URLPORTAL?r=".URI::Escape::uri_escape("/monkey".CGI::script_name())."&c=$codigo");
#		exit(0);
#	} elsif ($codigo) {
		$param{-status} = $codigo;
		print CGI::header(\%param),"\n";
		print CGI::start_html(-encoding => "utf-8");
#	}

	print CGI::h3("ERROR: $texto"),"\n";
	print CGI::end_html(),"\n";

	exit(0);
}


1;
