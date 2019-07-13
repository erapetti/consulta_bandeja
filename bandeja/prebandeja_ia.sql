/* Por ahora no tengo condiciones para comprobar en la prebandeja */

-- la liquidación se haría para el mes anterior
set @anio=if(month(@hastaBandeja)=1,-1,0)+year(@hastaBandeja);
set @mes=if(month(@hastaBandeja)=1,12,right(concat('0',month(@hastaBandeja)-1),2));


select concat('ya existe una liquidación para ',@anio,'-',@mes) causal,count(*) q
from ivariables
where VarAnio=@anio
  and VarMes=@mes
  and VarTipo='IRPF ABONOS'
;
