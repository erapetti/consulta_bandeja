#!/bin/bash
#
# reliquidar_mu.sh
#
#	Reliquidación de la bandeja de docencia indirecta


USO="uso: reliquidar_mu.sh --inicio yyyy-mm-aa --fin yyyy-mm-aa"

while [ -n "$1" ]
do
	if [ "$1" = "--inicio" -a -n "$2" ]
	then
		DESDE="$2"
		shift
	elif [ "$1" = "--fin" -a -n "$2" ]
	then
		HASTA="$2"
		shift
	else
		echo "$USO"
		exit 1
	fi
	shift
done

if [ -z "$DESDE" -o -z "$HASTA" ]
then
	echo "$USO"
	exit 2
fi


if ! date --date "$DESDE" 2>&1 > /dev/null
then
	echo ERROR: Fecha desde en formato incorrecto
	exit 3
fi

if ! date --date "$HASTA" 2>&1 > /dev/null
then
	echo ERROR: Fecha hasta en formato incorrecto
	exit 4
fi

if ! [ `date --date "$DESDE" +%s` -le `date --date "$HASTA" +%s` ]
then
	echo ERROR: La fecha desde debe ser menor o igual a la fecha hasta
	exit 5
fi


# Credenciales del host de CES:
HOST=sdb690-05.ces.edu.uy
USER=bandejamu
PASS=`echo -n T0ZOZTJaNWlYZzlSV2N0USs1d3IzeVBB | base64 -d`

# Credenciales del host de SIAP:
HOSTSIAP=sdb690-07.ces.edu.uy
USERSIAP=bandejamu
PASSSIAP=`echo -n T0ZOZTJaNWlYZzlSV2N0USs1d3IzeVBB | base64 -d`


PREBANDEJA=prebandeja_mu.sql
BANDEJA_ADM=bandeja_mu_ADM.sql
BANDEJA_DHF=bandeja_mu_DHF.sql
BANDEJA_DOC=bandeja_mu_DOC.sql
POSBANDEJA=posbandeja_mu.sql

MYSQL="mysql -h $HOST -u $USER -p$PASS --batch --skip-column-names"

OUT=`cat "$PREBANDEJA" | $MYSQL Personal 2>&1 | grep -v "Using a password on the command line"`

if [ -z "$OUT" ]
then
	# hubo error
	exit 6
fi

if [ `echo "$OUT" | grep -v '	0$' | wc -l` -gt 0 ]
then
	echo "ERROR: la prebandeja devolvió:
$OUT" | sed 's/\t/:/g'
	exit 7
fi

SQL1='
use siap_ces_tray;
CREATE TEMPORARY TABLE `imultastmp` (
  `PerDocTpo` char(2) DEFAULT NULL,
  `PerDocPaisCod` char(2) DEFAULT NULL,
  `PerDocNum` char(16) DEFAULT NULL,
  `MultCarNum` int(11) DEFAULT NULL,
  `RubroCod` char(16) DEFAULT NULL,
  `MultAnio` char(4) DEFAULT NULL,
  `MultMes` char(2) DEFAULT NULL,
  `MultCic` int(3) DEFAULT NULL,
  `MultCar` char(4) DEFAULT NULL,
  `MultInsCod` char(30) DEFAULT NULL,
  `MultAsiCod` int(3) DEFAULT NULL,
  `MultCantDias` int(2) DEFAULT NULL,
  `MultCantHor` int(2) DEFAULT NULL,
  `MultCantMin` int(4) DEFAULT NULL,
  `InasisLicId` int(11) NOT NULL,
  `InasisLicId_Orig` int(11) DEFAULT NULL
)
'

SQL2="set @desdeBandeja='$DESDE'; set @hastaBandeja='$HASTA';"

SQL3="use Personal;"`cat "$BANDEJA_ADM" | perl -ne 's/(start transaction|rollback|commit|set\s+\@desdeBandeja[^;]+|set\s+\@hastabandeja[^;]+|set\s+\@perid[^;]+)\s*;//ig;print'`
SQL4="use Personal;"`cat "$BANDEJA_DHF" | perl -ne 's/(start transaction|rollback|commit|set\s+\@desdeBandeja[^;]+|set\s+\@hastabandeja[^;]+|set\s+\@perid[^;]+)\s*;//ig;print'`
SQL5="use Personal;"`cat "$BANDEJA_DOC" | perl -ne 's/(start transaction|rollback|commit|set\s+\@desdeBandeja[^;]+|set\s+\@hastabandeja[^;]+|set\s+\@perid[^;]+)\s*;//ig;print'`

SQL7='select "(",
concat(char(34),PerDocTpo,char(34)),",",
concat(char(34),PerDocPaisCod,char(34)),",",
concat(char(34),PerDocNum,char(34)),",",
MultCarNum,",",
concat(char(34),RubroCod,char(34)),",",
concat(char(34),MultAnio,char(34)),",",
concat(char(34),MultMes,char(34)),",",
MultCic,",",
concat(char(34),MultCar,char(34)),",",
concat(char(34),MultInsCod,char(34)),",",
MultAsiCod,",",
MultCantDias,",",
MultCantHor,",",
MultCantMin,",",
concat(char(34),"PE",char(34)),",",
concat(char(34),char(34)),",",
concat(char(34),curdate(),char(34)),",",
InasisLicId,",",
InasisLicId_Orig,
"),"
from siap_ces_tray.imultastmp
'

# Saco ; al final de cada SQL
SQL1=`echo "$SQL1" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL2=`echo "$SQL2" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL3=`echo "$SQL3" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL4=`echo "$SQL4" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL5=`echo "$SQL5" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL8=`cat "$POSBANDEJA" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`

# Bandeja ADM

# Corro las sentencias en el servidor MySQL del Corporativo
RESULTADO_ADM=`echo "
start transaction;
$SQL1;
$SQL2;
$SQL3;
$SQL7;
commit;" | $MYSQL Personal 2>&1`

err=$?

if [ $err -ne 0 ]
then
	echo "ERROR MySQL ADM retorna código $err. $RESULTADO_ADM"
	exit 8
fi

if [ -z "$RESULTADO_ADM" ]
then
	echo ERROR La reliquidación ADM no produjo resultado
	exit 9
fi

# para evitar Gateway timeout
echo
date 1>&2

# Bandeja DHF

# Corro las sentencias en el servidor MySQL del Corporativo
RESULTADO_DHF=`echo "
start transaction;
$SQL1;
$SQL2;
$SQL4;
$SQL7;
commit;" | $MYSQL Personal 2>&1`

err=$?

if [ $err -ne 0 ]
then
	echo "ERROR MySQL DHF retorna código $err. $RESULTADO_DHF"
	exit 10
fi

if [ -z "$RESULTADO_DHF" ]
then
	echo ERROR La reliquidación DHF no produjo resultado
	exit 11
fi

# para evitar Gateway timeout
echo
date 1>&2

# Bandeja DOC

# Corro las sentencias en el servidor MySQL del Corporativo
RESULTADO_DOC=`echo "
start transaction;
$SQL1;
$SQL2;
$SQL5;
$SQL7;
commit;" | $MYSQL Personal 2>&1`

err=$?

if [ $err -ne 0 ]
then
	echo "ERROR MySQL DOC retorna código $err. $RESULTADO_DOC"
	exit 10
fi

if [ -z "$RESULTADO_DOC" ]
then
	echo ERROR La reliquidación DOC no produjo resultado
	exit 11
fi

# para evitar Gateway timeout
echo
date 1>&2


# Junto todo:

RESULTADO=`echo "$RESULTADO_ADM $RESULTADO_DHF $RESULTADO_DOC" | sed 's/mysql.*Using a password on the command line.*//;s/\t//g'`
RESULTADO=`echo "$RESULTADO" | perl -e 'local $/=undef; $_=<>; s/,[\s\n\r]*$//s; print'`

echo "start transaction;
$SQL2;
insert into imultas (PerDocTpo,PerDocPaisCod,PerDocNum,MultCarNum,RubroCod,MultAnio,MultMes,MultCic,MultCar,MultInsCod,MultAsiCod,MultCantDias,MultCantHor,MultCantMin,Resultado,Mensaje,MultFchCarga,InasisLicId,InasisLicId_Orig) values
$RESULTADO;
$SQL8;
insert into periodos (bandeja,desde,hasta) values ('mu','$DESDE','$HASTA');
commit;" | mysql -h "$HOSTSIAP" -u "$USERSIAP" "-p$PASSSIAP" siap_ces_tray 2>&1 | grep -v "Using a password on the command line"

exit 0
