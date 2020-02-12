#!/bin/bash
#
# reliquidar_di.sh
#
#	Reliquidación de la bandeja de docencia indirecta


USO="uso: reliquidar_di.sh --inicio yyyy-mm-aa --fin yyyy-mm-aa"

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

PREBANDEJA=prebandeja_di.sql
BANDEJA=bandeja2-v2.02.03-20200206.sql
POSBANDEJA=bandeja3-v1.20.02-20190322.sql

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
CREATE TEMPORARY TABLE `idesignaciones` (
  `DesigId` bigint(20) NOT NULL AUTO_INCREMENT,
  `EmpCod` smallint(6) NOT NULL,
  `PerDocTpo` char(2)  NOT NULL,
  `PerDocPaisCod` char(2)  DEFAULT NULL,
  `PerDocNum` char(16)  DEFAULT NULL,
  `CarNum` int(11) DEFAULT NULL,
  `RelLabId` int(11) DEFAULT NULL,
  `CarCod` int(11) DEFAULT NULL,
  `CarNumVer` smallint(6) DEFAULT NULL,
  `CarNiv` smallint(6) DEFAULT NULL,
  `EscCod` char(30)  DEFAULT NULL,
  `CatCod` smallint(6) DEFAULT NULL,
  `DesFchIng` date DEFAULT NULL,
  `DesFchEgr` date DEFAULT NULL,
  `FunCod` bigint(20) DEFAULT NULL,
  `DesActLabNum` smallint(6) DEFAULT NULL,
  `SegSalCod` smallint(6) DEFAULT NULL,
  `LiqUniCod` char(30)  DEFAULT NULL,
  `VinFunCod` smallint(6) DEFAULT NULL,
  `CauBajCod` smallint(6) DEFAULT NULL,
  `ComEspCod` smallint(6) DEFAULT NULL,
  `TipoRemCod` smallint(6) DEFAULT NULL,
  `DesInsCod` char(30)  DEFAULT NULL,
  `DesInsFchDesde` date DEFAULT NULL,
  `DesInsFchHasta` date DEFAULT NULL,
  `CarRegHor` decimal(4,2) DEFAULT NULL,
  `DesRegHor` decimal(4,2) DEFAULT NULL,
  `DesRegHorFchDesde` date DEFAULT NULL,
  `DeRegHorFchHasta` date DEFAULT NULL,
  `DesCarId` mediumint(9) DEFAULT NULL,
  `DesCarCod` smallint(6) DEFAULT NULL,
  `ForAccCarCod` smallint(6) DEFAULT NULL,
  `DesGraEsp` smallint(6) DEFAULT NULL,
  `DesFchFinCon` date DEFAULT NULL,
  `DesObs` varchar(200)  DEFAULT NULL,
  `DesFchIniCon` date DEFAULT NULL,
  `SitFunId` int(4) DEFAULT NULL,
  `SitFunFchDesde` date DEFAULT NULL,
  `SitFunFchHasta` date DEFAULT NULL,
  `LiqGruCod` smallint(6) DEFAULT NULL,
  `DesFunCod` mediumint(9) DEFAULT NULL,
  `DesFchCarga` date DEFAULT NULL,
  `DesFchProc` date DEFAULT NULL,
  `Resultado` varchar(20)  DEFAULT NULL,
  `Mensaje` varchar(100)  DEFAULT NULL,
  `NroLote` int(11) DEFAULT NULL,
  PRIMARY KEY (`DesigId`)
);
';

SQL2="set @desdeBandeja='$DESDE'; set @hastaBandeja='$HASTA';"

SQL3=`cat "$BANDEJA" | perl -ne 's/(start transaction|rollback|commit|set\s+\@desdeBandeja[^;]+|set\s+\@hastabandeja[^;]+|set\s+\@perid[^;]+)\s*;//ig;print'`


SQL4='
select "insert into idesignaciones values (",
null,",",
EmpCod,",",
if(PerDocTpo is not null,concat(char(34),PerDocTpo,char(34)),null),",",
if(PerDocPaisCod is not null,concat(char(34),PerDocPaisCod,char(34)),null),",",
if(PerDocNum is not null,concat(char(34),PerDocNum,char(34)),null),",",
CarNum,",",
RelLabId,",",
CarCod,",",
CarNumVer,",",
CarNiv,",",
if(EscCod is not null,concat(char(34),EscCod,char(34)),null),",",
CatCod,",",
if(DesFchIng is not null,concat(char(34),DesFchIng,char(34)),null),",",
if(DesFchEgr is not null,concat(char(34),DesFchEgr,char(34)),null),",",
FunCod,",",
DesActLabNum,",",
SegSalCod,",",
if(LiqUniCod is not null,concat(char(34),LiqUniCod,char(34)),null),",",
VinFunCod,",",
CauBajCod,",",
ComEspCod,",",
TipoRemCod,",",
if(DesInsCod is not null,concat(char(34),DesInsCod,char(34)),null),",",
if(DesInsFchDesde is not null,concat(char(34),DesInsFchDesde,char(34)),null),",",
if(DesInsFchHasta is not null,concat(char(34),DesInsFchHasta,char(34)),null),",",
CarRegHor,",",
DesRegHor,",",
if(DesRegHorFchDesde is not null,concat(char(34),DesRegHorFchDesde,char(34)),null),",",
if(DeRegHorFchHasta is not null,concat(char(34),DeRegHorFchHasta,char(34)),null),",",
DesCarId,",",
DesCarCod,",",
ForAccCarCod,",",
DesGraEsp,",",
if(DesFchFinCon is not null,concat(char(34),DesFchFinCon,char(34)),null),",",
if(DesObs is not null,concat(char(34),DesObs,char(34)),null),",",
if(DesFchIniCon is not null,concat(char(34),DesFchIniCon,char(34)),null),",",
SitFunId,",",
if(SitFunFchDesde is not null,concat(char(34),SitFunFchDesde,char(34)),null),",",
if(SitFunFchHasta is not null,concat(char(34),SitFunFchHasta,char(34)),null),",",
LiqGruCod,",",
DesFunCod,",",
if(DesFchCarga is not null,concat(char(34),DesFchCarga,char(34)),null),",",
if(DesFchProc is not null,concat(char(34),DesFchProc,char(34)),null),",",
if(Resultado is not null,concat(char(34),Resultado,char(34)),null),",",
if(Mensaje is not null,concat(char(34),Mensaje,char(34)),null),",",
NroLote,");"
from siap_ces_tray.idesignaciones;
'


# Saco ; al final de cada SQL
SQL1=`echo "$SQL1" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL2=`echo "$SQL2" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL3=`echo "$SQL3" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL4=`echo "$SQL4" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`
SQL5=`cat "$POSBANDEJA" | perl -e 'local $/=undef; $_=<>; s/;[\s\n\r]*$//s; print'`

# Corro las sentencias en el servidor MySQL
RESULTADO=`echo "$SQL1;
$SQL2;
$SQL3;
$SQL4;
$SQL5;" | $MYSQL Personal 2>&1`

err=$?

RESULTADO=`echo "$RESULTADO" | sed 's/mysql.*Using a password on the command line.*//;s/	//g'`

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

echo "start transaction;
$RESULTADO
insert into periodos (bandeja,desde,hasta) values ('di','$DESDE','$HASTA');
commit;" | mysql -h "$HOSTSIAP" -u "$USERSIAP" "-p$PASSSIAP" siap_ces_tray 2>&1 | grep -v "Using a password on the command line"

exit 0
