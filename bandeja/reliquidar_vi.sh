#!/bin/bash
#
# reliquidar_vi.sh
#
#	Reliquidación de la bandeja de docencia indirecta


USO="uso: reliquidar_vi.sh --inicio yyyy-mm-aa --fin yyyy-mm-aa"

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
USER=bandejavi
PASS=`echo -n T0ZOZTJaNWlYZzlSV2N0USs1d3IzeVBB | base64 -d`

# Credenciales del host de SIAP:
HOSTSIAP=sdb690-07.ces.edu.uy
USERSIAP=bandejavi
PASSSIAP=`echo -n T0ZOZTJaNWlYZzlSV2N0USs1d3IzeVBB | base64 -d`


PREBANDEJA=prebandejaVI-v1.0.0-20190413.sql
BANDEJA=bandejaVI-v1.0.3-20190604.sql
POSBANDEJA=posbandejaVI-v1.0.0-20190413.sql

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
CREATE TEMPORARY TABLE `iviaticos` (
  `ViatId` bigint(20) NOT NULL AUTO_INCREMENT,
  `PerDocTpo` char(2) DEFAULT NULL,
  `PerDocPaisCod` char(2) DEFAULT NULL,
  `PerDocNum` char(16) DEFAULT NULL,
  `CodConcepto` char(16) DEFAULT NULL,
  `ViatAnio` char(4) DEFAULT NULL,
  `ViatMes` char(2) DEFAULT NULL,
  `ViatCant` int(2) DEFAULT NULL,
  `ViaticoComun` int(2) DEFAULT NULL,
  `ViatFchCarga` date DEFAULT NULL,
  `Resultado` varchar(20) DEFAULT NULL,
  `Mensaje` varchar(100) DEFAULT NULL,
  `ViatFchProc` date DEFAULT NULL,
  `NroLote` int(11) DEFAULT NULL,
  `ViatIdRef` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`ViatId`) USING BTREE
);
';


SQL2="set @desdeBandeja='$DESDE'; set @hastaBandeja='$HASTA';"

SQL3=`cat "$BANDEJA" | perl -ne 's/(start transaction|rollback|commit|set\s+\@desdeBandeja[^;]+|set\s+\@hastabandeja[^;]+|set\s+\@perid[^;]+)\s*;//ig;print'`

SQL4='
select "insert into iviaticos (ViatId,PerDocTpo,PerDocPaisCod,PerDocNum,CodConcepto,ViatAnio,ViatMes,ViatCant,ViaticoComun,ViatFchCarga,Resultado,Mensaje,ViatFchProc,NroLote,ViatIdRef) values (",
null,",",
if(PerDocTpo is not null,concat(char(34),PerDocTpo,char(34)),null),",",
if(PerDocPaisCod is not null,concat(char(34),PerDocPaisCod,char(34)),null),",",
if(PerDocNum is not null,concat(char(34),PerDocNum,char(34)),null),",",
CodConcepto,",",
concat(char(34),ViatAnio,char(34)),",",
concat(char(34),ViatMes,char(34)),",",
ViatCant,",",
ViaticoComun,",",
if(ViatFchCarga is not null,concat(char(34),ViatFchCarga,char(34)),null),",",
if(Resultado is not null,concat(char(34),Resultado,char(34)),null),",",
if(Mensaje is not null,concat(char(34),Mensaje,char(34)),null),",",
if(ViatFchProc is not null,concat(char(34),ViatFchProc,char(34)),null),",",
NroLote,",",
ViatIdRef,");"
from siap_ces_tray.iviaticos;
'

# Saco ; al final de cada SQL
SQL1=`echo "$SQL1" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL2=`echo "$SQL2" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL3=`echo "$SQL3" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL4=`echo "$SQL4" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL5=`cat "$POSBANDEJA" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`

# Corro las sentencias en el servidor MySQL del Corporativo
RESULTADO=`echo "$SQL1;
$SQL2;
$SQL3;
$SQL4;
$SQL5;" | $MYSQL Personal 2>&1`

err=$?

RESULTADO=`echo "$RESULTADO" | sed 's/mysql.*Using a password on the command line.*//;s/\t//g'`

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

echo "$RESULTADO" | mysql -h "$HOSTSIAP" -u "$USERSIAP" "-p$PASSSIAP" siap_ces_tray 2>&1 | grep -v "Using a password on the command line"

exit 0
