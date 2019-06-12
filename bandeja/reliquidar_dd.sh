#!/bin/bash
#
# reliquidar_dd.sh
#
#	Reliquidación de la bandeja de docencia directa


USO="uso: reliquidar_dd.sh --inicio yyyy-mm-aa --fin yyyy-mm-aa [ --ci cedula ]"

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
	elif [ "$1" = "--ci" -a -n "$2" ]
	then
		CI="$2"
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
USER=bandeja_dd
PASS=`echo -n b0syK2FRTXErb3ZxTTBweHRidDlwSHFGUVQ5OUxxa01idWsxamIrRDk5RT0= | base64 -d`

# Credenciales del host de SIAP:
HOSTSIAP=sdb690-07.ces.edu.uy
USERSIAP=bandeja_dd
PASSSIAP=`echo -n b0syK2FRTXErb3ZxTTBweHRidDlwSHFGUVQ5OUxxa01idWsxamIrRDk5RT0= | base64 -d`


PREBANDEJA=prebandejadd-v1.0.0-20190415.sql
BANDEJA=bj-9.0.5.py
POSBANDEJA=posbandejadd-v1.0.1-20190426.sql

MYSQL="mysql -h $HOST -u $USER -p$PASS --batch --skip-column-names"
MYSQLSIAP="mysql -h $HOSTSIAP -u $USERSIAP -p$PASSSIAP --batch --skip-column-names"

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


ARGS="--inicio $DESDE --fin $HASTA"
if [ -n "$CI" ]
then
	ARGS="$ARGS --ci $CI"
fi


RESULTADO=`/usr/bin/python -W ignore $BANDEJA $ARGS`
err=$?

if [ $err -ne 0 ]
then
	echo "ERROR MySQL retorna código $err. $RESULTADO"
	exit 8
fi

if [ -z "$CI" ]
then
	SQL='set @ci="";'
else
	SQL='set @ci="'$CI'";'
fi

SQL="start transaction; $SQL"`cat "$POSBANDEJA"`"commit;"


RESULTADO=`echo "$SQL" | $MYSQLSIAP siap_ces_tray 2>&1`
err=$?
RESULTADO=`echo "$RESULTADO" | sed 's/mysql.*Using a password on the command line.*//'`

if [ $err -ne 0 ]
then
	echo "ERROR MySQL retorna código $err. $RESULTADO"
	exit 9
fi


exit 0
