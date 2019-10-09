#!/bin/bash
#
# reliquidar_ia.sh
#
#	Reliquidación de la bandeja de docencia indirecta


USO="uso: reliquidar_ia.sh --inicio yyyy-mm-aa --fin yyyy-mm-aa"

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


# Cargo HOST, USER, PASS, HOSTSIAP, USERSIAP, PASSSIAP
source credenciales.sh

PREBANDEJA=prebandeja_ia.sql
BANDEJA=bandeja_ia.sql
POSBANDEJA=posbandeja_ia.sql

MYSQL="mysql -h $HOST -u $USER -p$PASS --batch --skip-column-names"

OUT=`cat "$PREBANDEJA" | mysql -h "$HOSTSIAP" -u "$USERSIAP" "-p$PASSSIAP" --batch --skip-column-names siap_ces_tray 2>&1 | grep -v "Using a password on the command line"`

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
CREATE TEMPORARY TABLE `ivariablestmp` (
  `PerDocTpo` char(2) DEFAULT NULL,
  `PerDocPaisCod` char(2) DEFAULT NULL,
  `PerDocNum` char(16) DEFAULT NULL,
  `VarCarNum` int(11) DEFAULT NULL,
  `VarConNum` char(16) DEFAULT NULL,
  `VarAnio` char(4) DEFAULT NULL,
  `VarMes` char(2) DEFAULT NULL,
  `VarImporte` decimal(7,2) DEFAULT "0.00"
)
'

SQL2="set @desdeBandeja='$DESDE'; set @hastaBandeja='$HASTA';"

SQL3="use Personal;"`cat "$BANDEJA" | perl -ne 's/(start transaction|rollback|commit|set\s+\@desdeBandeja[^;]+|set\s+\@hastabandeja[^;]+|set\s+\@perid[^;]+)\s*;//ig;print'`

SQL4='select "(",
concat(char(34),PerDocTpo,char(34)),",",
concat(char(34),PerDocPaisCod,char(34)),",",
concat(char(34),PerDocNum,char(34)),",",
VarCarNum,",",
concat(char(34),VarConNum,char(34)),",",
concat(char(34),VarAnio,char(34)),",",
concat(char(34),VarMes,char(34)),",",
VarImporte,",",
concat(char(34),"PE",char(34)),",",
concat(char(34),char(34)),",",
concat(char(34),curdate(),char(34)),",",
concat(char(34),"IRPF-ABONOS",char(34)),",",
concat(char(34),VarAnio,char(34)),",",
concat(char(34),VarMes,char(34)),
"),"
from siap_ces_tray.ivariablestmp
'

# Saco ; al final de cada SQL
SQL1=`echo "$SQL1" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL2=`echo "$SQL2" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL3=`echo "$SQL3" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL5=`cat "$POSBANDEJA" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`

# Corro las sentencias en el servidor MySQL del Corporativo
RESULTADO=`echo "
start transaction;
$SQL1;
$SQL2;
$SQL3;
$SQL4;
$SQL5;
commit;" | $MYSQL Personal 2>&1`

err=$?

if [ $err -ne 0 ]
then
	echo "ERROR MySQL retorna código $err. $RESULTADO"
	exit 8
fi

if [ -z "$RESULTADO" ]
then
	echo ERROR La reliquidación no produjo resultado
	exit 9
fi

# para evitar Gateway timeout
echo
date 1>&2


# Junto todo:

RESULTADO=`echo "$RESULTADO" | sed 's/mysql.*Using a password on the command line.*//;s/\t//g'`
RESULTADO=`echo "$RESULTADO" | perl -e 'local $/=undef; $_=<>; s/,[\s\n\r]*$//s; print'`

echo "start transaction;
insert into ivariables (PerDocTpo,PerDocPaisCod,PerDocNum,VarCarNum,VarConNum,VarAnio,VarMes,VarImporte,Resultado,Mensaje,VarFchCarga,VarTipoVariable,VarAnioRel,VarMesRel) values
$RESULTADO;
insert into periodos (bandeja,desde,hasta) values ('ia','$DESDE','$HASTA');
commit;" | mysql -h "$HOSTSIAP" -u "$USERSIAP" "-p$PASSSIAP" siap_ces_tray 2>&1 | grep -v "Using a password on the command line"

exit 0
