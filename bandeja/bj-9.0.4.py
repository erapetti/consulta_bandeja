# coding: utf-8
#!/usr/bin/python
import sqlalchemy as sa
from sqlalchemy import create_engine, func, Date, cast, Integer
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import literal_column
import pandas as pd
import pymysql
import datetime as dt
from datetime import date
import sys
import yaml
import numpy as np
import click
import time

@click.command()
@click.option('--inicio', help = 'Fecha de inicio de las novedades')
@click.option('--fin', help = 'Fecha de fin de las novedades')
@click.option('--ci',help = "ej : ci='33613521', considera los datos de salida solo para la cedula de identidad indicada")

def main(inicio,fin,ci):

    """
    En el archivo bandejas.cfg se guardan los parametros de configuracion para la base de datos. El procedimiento toma el archivo de desde el mismo directorio donde se encuentra. En el archivo config.cfg se guardan parametros de configuracion.
    \nEjecucion
    El procedimiento se ejecuta de la siguiente forma:
    (ejemplo)\n
    $python bj.py --inicio='2018-05-01' --fin='2018-05-14'

        - Novedades en el periodo [inicio, fin) , incluyendo inicio y no incluye la fecha de fin.Las novedades se refiere a las altas de designacion, ceses de designacion, anulaciones y pasajes a suplencias
        - Para todas las personas (PerId) que tuvieron novedades en el periodo indicado, se toman los datos de toda la historia de altas, ceses, con tope el 01/03 del año correspondiente a la fecha de inicio que es pasada como parametro.
    """

    with open('bandejas.cfg', 'r') as ymlfile:
        cdb = yaml.load(ymlfile)

    with open('config.cfg', 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    engine = create_engine('mysql+pymysql://'+cdb['personal']['usr']+':'+cdb['personal']['password']+'@'+cdb['personal']['host']+'/'+cdb['personal']['bd'])
    engine_bandeja_in = create_engine('mysql+pymysql://'+cdb['bandeja_in']['usr']+':'+cdb['bandeja_in']['password']+'@'+cdb['bandeja_in']['host']+'/'+cdb['bandeja_in']['bd'])
    engine_bandeja_out = create_engine('mysql+pymysql://'+cdb['bandeja_out']['usr']+':'+cdb['bandeja_out']['password']+'@'+cdb['bandeja_out']['host']+'/'+cdb['bandeja_out']['bd'])


    puestos_funcion = cfg['puestos_funcion'] # los puestos considerados docencia directa
    parametros = {}
    parametros['p1d']=dt.date(int(inicio.split('-')[0]),int(inicio.split('-')[1]),int(inicio.split('-')[2]))
    parametros['p2d']=dt.date(int(fin.split('-')[0]),int(fin.split('-')[1]),int(fin.split('-')[2]))

    # no voy a dejar pasar designaciones que inicien a partir de este tope (el mes siguiente al dado como fin)
    parametros['tope']=dt.date(int(fin.split('-')[0])+(1 if (fin.split('-')[1]=='12') else 0),1 if (fin.split('-')[1]=='12') else int(fin.split('-')[1])+1,1)


    # las causales de suplencia que interesan
    suplcausales = cfg['suplcausales']

    parametros['inicioLectivo'] = dt.datetime(int(inicio.split('-')[0])-(1 if inicio.split('-')[1]<'03' else 0), 03, 01)

    #cargo metadatos del modelo Personal
    metadata = sa.MetaData()
    relaciones_laborales = sa.Table('RELACIONES_LABORALES',metadata,autoload=True, autoload_with=engine)
    anulaciones = sa.Table('ANULACIONES', metadata,autoload=True,autoload_with=engine)
    funciones_relacion_laboral = sa.Table('FUNCIONES_RELACION_LABORAL', metadata, autoload=True, autoload_with=engine)
    funciones_asignadas = sa.Table('FUNCIONES_ASIGNADAS', metadata, autoload=True, autoload_with=engine)
    sillas = sa.Table('SILLAS', metadata, autoload=True, autoload_with=engine)
    cargas_horarias = sa.Table('CARGAS_HORARIAS', metadata, autoload=True, autoload_with=engine)
    silla_grupo_materia = sa.Table('SILLAGRUPOMATERIA', metadata, autoload=True, autoload_with=engine)
    puestos = sa.Table('PUESTOS', metadata, autoload=True, autoload_with=engine)
    denominaciones_cargo= sa.Table('DENOMINACIONES_CARGOS', metadata, autoload=True, autoload_with=engine)
    suplencias = sa.Table('SUPLENCIAS', metadata, autoload=True, autoload_with=engine)
    funciones_agrup_lin = sa.Table('FUNCION_AGRUP_LIN', metadata, autoload=True, autoload_with=engine)

    # cargo metadatos de Personas
    personas = sa.Table('PERSONAS', metadata, schema="Personas", autoload=True, autoload_with=engine)
    personas_documentos = sa.Table('PERSONASDOCUMENTOS', metadata, schema="Personas", autoload=True, autoload_with=engine)

    # cargo los datos de materias de estudiantil
    asignaturas_materias = sa.Table('ASIGNATURAS_MATERIAS', metadata, schema="Estudiantil", autoload=True, autoload_with=engine)

    # cargo las materias de estudiantil
    query_asignaturas_materias = sa.select([asignaturas_materias])
    df_asignaturas_materias = pd.read_sql_query(query_asignaturas_materias, engine, params=parametros)

    # cargo los datos de la base de siap para las dependencias
    tabla_institucional = sa.Table('tabla_institucional',metadata, autoload=True, autoload_with=engine_bandeja_in)
    query_tabla_institucional = sa.select([tabla_institucional.c.DEP_AS400.label('dependid'),tabla_institucional.c.DEP_DBC.label('dependidSiap')]).select_from(tabla_institucional);
    df_tabla_institucional = pd.read_sql_query(query_tabla_institucional, engine_bandeja_in, params=parametros)

    # cargo las funciones para identificar las horas de apoyo o POB , POP, talleristas , codigo 68
    query_funciones_cargo = sa.select([funciones_agrup_lin])
    df_funciones_cargo = pd.read_sql_query(query_funciones_cargo, engine,params=parametros)
    df_funciones_hap = df_funciones_cargo.loc[df_funciones_cargo.Funcion_Agrup_Cab_Id==1, 'FuncionId']
    df_funciones_POB = df_funciones_cargo.loc[df_funciones_cargo.Funcion_Agrup_Cab_Id==8, 'FuncionId']
    df_funciones_POP = df_funciones_cargo.loc[df_funciones_cargo.Funcion_Agrup_Cab_Id==7, 'FuncionId']
    df_funciones_68  = df_funciones_cargo.loc[df_funciones_cargo.Funcion_Agrup_Cab_Id==5, 'FuncionId']
    df_funciones_talleristas  = df_funciones_cargo.loc[df_funciones_cargo.Funcion_Agrup_Cab_Id==9, 'FuncionId']
    df_coordinadores_especiales = df_funciones_cargo.loc[df_funciones_cargo.Funcion_Agrup_Cab_Id==10,'FuncionId']

    # novedades
    query_novedades = sa. \
        select([relaciones_laborales.c.PersonalPerId, relaciones_laborales.c.RelLabId]). \
        select_from(relaciones_laborales.join(puestos)). \
        where( \
              (puestos.c.PuestoFuncionId.in_(puestos_funcion)) & \
              # RL designada
              (relaciones_laborales.c.PersonalPerId <> None) & \
              ( \
               # se inicia en el período de la bandeja
               ( \
                (relaciones_laborales.c.RelLabFchIniActividades >= sa.bindparam('p1d')) & \
                (relaciones_laborales.c.RelLabFchIniActividades < sa.bindparam('p2d')) \
               ) | \
               # o termina en el período de la bandeja
               ( \
                (relaciones_laborales.c.RelLabCeseFchReal >= sa.bindparam('p1d')) & \
                (relaciones_laborales.c.RelLabCeseFchReal < sa.bindparam('p2d')) \
               ) | \
               # o cambiaron el alta con retraso
               ( \
                (relaciones_laborales.c.RelLabDesignFchAlta >= sa.bindparam('p1d')) & \
                (relaciones_laborales.c.RelLabDesignFchAlta < sa.bindparam('p2d')) \
               ) | \
               # o cambiaron el cese con retraso
               ( \
                (relaciones_laborales.c.RelLabCeseFchAlta >= sa.bindparam('p1d')) & \
                (relaciones_laborales.c.RelLabCeseFchAlta < sa.bindparam('p2d'))
               ) \
              ) \
             )
    df_novedades = pd.read_sql_query(query_novedades, engine, params=parametros)

    # cargo las anulaciones del periodo
    query_anulaciones_periodo = sa. \
        select([relaciones_laborales.c.PersonalPerId,relaciones_laborales.c.RelLabId, anulaciones.c.AnulacionFchAlta]). \
        select_from(anulaciones.join(relaciones_laborales, cast(anulaciones.c.AnulacionValorPkTabla,Integer)==relaciones_laborales.c.RelLabId).join(puestos)). \
        where( \
              (anulaciones.c.AnulacionFchAlta >= sa.bindparam('p1d')) & \
              (anulaciones.c.AnulacionFchAlta < sa.bindparam('p2d')) & \
              (anulaciones.c.AnulacionTipoNombre=='DESIGNACION') & \
              (puestos.c.PuestoFuncionId.in_(puestos_funcion)) \
             )
    df_anulaciones_periodo = pd.read_sql(query_anulaciones_periodo,engine,params=parametros)

    rlt = aliased(relaciones_laborales) # RL de los titulares
    rls = aliased(relaciones_laborales) # RL de los suplentes

    # perids que tuvieron novedades o tienen eventos en el período de la bandeja (o el que vino de parámetro)
    if ci!=None: # si me pasaron una ci como parametro me interesan solo las novedades de esa ci
        query_perid = sa.select([personas_documentos.c.PerId]).select_from(personas_documentos).where((personas_documentos.c.PaisCod=='UY')&(personas_documentos.c.DocCod=='CI')&(personas_documentos.c.PerDocId==ci))
        set_perids_novedades = pd.read_sql_query(query_perid, engine, params=parametros)['PerId'].unique().tolist()
    else:
        # cargo las suplencias del período
        query_suplencias = sa. \
            select([rlt.c.PersonalPerId,suplencias.c.RelLabId,func.GREATEST(cast(suplencias.c.SuplFchAlta,Date),rlt.c.RelLabFchIniActividades).label('SuplFchAlta'),suplencias.c.SuplCausId,rlt.c.RelLabFchIniActividades,rlt.c.RelLabCeseFchReal,rls.c.RelLabAnulada.label('RelLabAnuladaS'),rls.c.RelLabFchIniActividades.label('RelLabFchIniActividadesS'),rls.c.RelLabCeseFchReal.label('RelLabCeseFchRealS')]). \
            select_from(rlt.join(puestos).join(suplencias, suplencias.c.RelLabId==rlt.c.RelLabId).join(rls, rls.c.RelLabId==suplencias.c.SuplRelLabId)). \
            where((puestos.c.PuestoFuncionId.in_(puestos_funcion)) & \
                  (suplencias.c.SuplCausId.in_(suplcausales)) & \
                  (rlt.c.RelLabAnulada==0) & \
                  ((rlt.c.RelLabFchIniActividades < rlt.c.RelLabCeseFchReal) | (rlt.c.RelLabCeseFchReal==None)) & \
                  # la rls podría estar anulada y en ese caso se marca la novedad en RelLabCeseFchAlta
                  ( \
                   # inicio de la suplencia está en el período de la bandeja:
                   ((func.GREATEST(cast(suplencias.c.SuplFchAlta,Date),rlt.c.RelLabFchIniActividades) < sa.bindparam('p2d')) & \
                    (func.GREATEST(cast(suplencias.c.SuplFchAlta,Date),rlt.c.RelLabFchIniActividades) >= sa.bindparam('p1d')) \
                   ) | \
                   # o el inicio de la suplencia fue modificado en el período de la bandeja:
                   ((cast(suplencias.c.Suplencias_FchUltAct,Date) < sa.bindparam('p2d')) & \
                    (cast(suplencias.c.Suplencias_FchUltAct,Date) >= sa.bindparam('p1d')) \
                   ) | \
                   # o el fin de la suplencia está en el período de la bandeja:
                   (((rls.c.RelLabCeseFchReal < sa.bindparam('p2d')) | (rls.c.RelLabCeseFchReal==None)) & \
                    (rls.c.RelLabCeseFchReal >= sa.bindparam('p1d')) \
                   ) | \
                   # o el fin de la suplencia fue modificado o anulado en el período de la bandeja:
                   ((rls.c.RelLabCeseFchAlta < sa.bindparam('p2d')) & \
                    (rls.c.RelLabCeseFchAlta >= sa.bindparam('p1d')) \
                   ) \
                  ) \
                 )
        df_suplencias = pd.read_sql_query(query_suplencias, engine, params=parametros)

        set_perids_novedades = df_novedades['PersonalPerId'].append(df_anulaciones_periodo['PersonalPerId']).append(df_suplencias['PersonalPerId']).unique().tolist()

    if len(set_perids_novedades) == 0: #si no tengo cédulas para procesar
       return

    ## Tomo la historia de los perid con novedades
    # join historia básica
    j3 = rlt.join(puestos).join(funciones_relacion_laboral).join(funciones_asignadas).join(sillas).join(silla_grupo_materia, sillas.c.SillaId==silla_grupo_materia.c.SillaId, isouter=True).join(asignaturas_materias, sillas.c.MateriaId==asignaturas_materias.c.MateriaId, isouter=True)

    # join suplencias
    jsupl = suplencias.join(rls, ((rls.c.RelLabId==suplencias.c.SuplRelLabId) & (rls.c.RelLabAnulada==0) & (suplencias.c.SuplCausId.in_(suplcausales))))
    # clone de join suplencias para encontrar la siguiente
    supl_siguiente  = aliased(suplencias) # suplencia consecutiva a la actual
    rls_siguiente   = aliased(relaciones_laborales)
    jsupl_siguiente = supl_siguiente.join(rls_siguiente, ((rls_siguiente.c.RelLabId==supl_siguiente.c.SuplRelLabId) & (rls_siguiente.c.RelLabAnulada==0) & (supl_siguiente.c.SuplCausId.in_(suplcausales))))
    # clone de join suplencias para asegurar que no hay una intermedia entre la actual y la siguiente
    supl_intermedia  = aliased(suplencias) # suplencia consecutiva a la actual
    rls_intermedia   = aliased(relaciones_laborales)
    jsupl_intermedia = supl_intermedia.join(rls_intermedia, ((rls_intermedia.c.RelLabId==supl_intermedia.c.SuplRelLabId) & (rls_intermedia.c.RelLabAnulada==0) & (supl_intermedia.c.SuplCausId.in_(suplcausales))))

    # historia básica de los perids con novedades, no incluye RL bajadas a suplencia
    query_historia_rl = sa. \
        select([rlt.c.PersonalPerId, puestos.c.PuestoFuncionId,rlt.c.RelLabId,rlt.c.RelLabDesignCaracter,rlt.c.RelLabCicloPago,rlt.c.RelLabFchIniActividades, rlt.c.RelLabCeseFchReal, rlt.c.CauBajCod,silla_grupo_materia.c.GrupoMateriaId,sillas.c.TurnoId, sillas.c.SillaDependId,funciones_relacion_laboral.c.FuncRelLabCantHrs,sillas.c.FuncionId,rlt.c.RelLabAnulada,puestos.c.PuestoAsignId,asignaturas_materias.c.AsignId]). \
        select_from( \
            j3. \
            join(jsupl, ((rlt.c.RelLabId==suplencias.c.RelLabId)), isouter=True) \
        ). \
        where((rlt.c.RelLabFchIniActividades >= sa.bindparam('inicioLectivo')) & \
              (rlt.c.PersonalPerId.in_(set_perids_novedades)) & \
              (puestos.c.PuestoFuncionId.in_(puestos_funcion)) & \
              (suplencias.c.RelLabId==None) \
             )
    df_historia_rl = pd.read_sql_query(query_historia_rl, engine, params=parametros)
    df_historia_rl.loc[:,'Origen']=['df_historia_rl']

    # Cambio el número de asignatura de las Coordinaciones
    df_historia_rl.loc[df_historia_rl['AsignId']==90,['AsignId','RelLabDesignCaracter']] = [75,'I']
    # Cambio el número de asignatura de AAM
    df_historia_rl.loc[df_historia_rl['AsignId']==98,'AsignId'] = 77


    # SUPLENCIAS
    # Para cada bajada a suplencia implica (recorriéndolas en orden de fecha) hay que:
    #  (1) agregar un registro desde el fin de la suplencia hasta el final original (luego el paso 2 le puede cambiar el cese)
    #  (2) cesar la RL vigente en la fecha de inicio de la suplencia
    #  (3) si el causal de bajada corresponde, hay que crear un registro (alta) para el período de suplencia paga

    # (1) altas inyectadas en la bandeja para el período posterior a cada licencia
    query_alta_luego_de_suplencia = sa. \
        select([rlt.c.PersonalPerId,puestos.c.PuestoFuncionId,rlt.c.RelLabId,rlt.c.RelLabDesignCaracter,rlt.c.RelLabCicloPago,func.GREATEST(rlt.c.RelLabFchIniActividades,func.ADDDATE(rls.c.RelLabCeseFchReal,1)).label('RelLabFchIniActividades'),func.IF(supl_siguiente.c.SuplId==None,rlt.c.RelLabCeseFchReal,cast(supl_siguiente.c.SuplFchAlta,Date)).label('RelLabCeseFchReal'),func.IF(supl_siguiente.c.SuplId==None,rlt.c.CauBajCod,'50').label('CauBajCod'),silla_grupo_materia.c.GrupoMateriaId,sillas.c.TurnoId,sillas.c.SillaDependId,funciones_relacion_laboral.c.FuncRelLabCantHrs,sillas.c.FuncionId,rlt.c.RelLabAnulada,puestos.c.PuestoAsignId,asignaturas_materias.c.AsignId]). \
        select_from( \
            jsupl. \
            join(j3, ((rlt.c.RelLabId==suplencias.c.RelLabId) & (rlt.c.RelLabAnulada==0))). \
            join(jsupl_siguiente, 
                 ((supl_siguiente.c.RelLabId==rlt.c.RelLabId) & (supl_siguiente.c.SuplId<>suplencias.c.SuplId) & (supl_siguiente.c.SuplFchAlta>=suplencias.c.SuplFchAlta)), \
                 isouter=True). \
            join(jsupl_intermedia, \
                 ((supl_intermedia.c.RelLabId==rlt.c.RelLabId) & (supl_intermedia.c.SuplId<>suplencias.c.SuplId) & (supl_intermedia.c.SuplFchAlta>=suplencias.c.SuplFchAlta) & (supl_intermedia.c.SuplId<>supl_siguiente.c.SuplId) & (supl_intermedia.c.SuplFchAlta<=supl_siguiente.c.SuplFchAlta)), \
                 isouter=True) \
        ). \
        where( \
            (rlt.c.RelLabFchIniActividades >= sa.bindparam('inicioLectivo')) & \
            (rlt.c.PersonalPerId.in_(set_perids_novedades)) & \
            (puestos.c.PuestoFuncionId.in_(puestos_funcion)) & \
            (rls.c.RelLabCeseFchReal<>None)  & \
            (supl_intermedia.c.SuplId==None) & \
            ((supl_siguiente.c.SuplId==None) | ((rls.c.RelLabCeseFchReal<>None) & (cast(supl_siguiente.c.SuplFchAlta,Date) > rls.c.RelLabCeseFchReal))) & \
            (func.ADDDATE(rls.c.RelLabCeseFchReal,1) < func.IF(supl_siguiente.c.SuplId==None,rlt.c.RelLabCeseFchReal,cast(supl_siguiente.c.SuplFchAlta,Date))) \
        )
    df_alta_luego_de_suplencia = pd.read_sql_query(query_alta_luego_de_suplencia, engine, params=parametros)
    df_alta_luego_de_suplencia.loc[:,'Origen']=['df_alta_luego_de_suplencia']

    # (2) alta inyectada para el período antes de la primer licencia
    query_primera_suplencia = sa. \
        select([rlt.c.PersonalPerId,puestos.c.PuestoFuncionId,rlt.c.RelLabId,rlt.c.RelLabDesignCaracter,rlt.c.RelLabCicloPago,rlt.c.RelLabFchIniActividades,cast(suplencias.c.SuplFchAlta,Date).label('RelLabCeseFchReal'),literal_column('50').label('CauBajCod'),silla_grupo_materia.c.GrupoMateriaId,sillas.c.TurnoId,sillas.c.SillaDependId,funciones_relacion_laboral.c.FuncRelLabCantHrs,sillas.c.FuncionId,rlt.c.RelLabAnulada,puestos.c.PuestoAsignId, asignaturas_materias.c.AsignId]). \
        select_from(
            jsupl. \
            join(j3, ((rlt.c.RelLabId==suplencias.c.RelLabId) & (rlt.c.RelLabAnulada==0))). \
            join(jsupl_intermedia, \
                 ((supl_intermedia.c.RelLabId==rlt.c.RelLabId) & (supl_intermedia.c.SuplId<>suplencias.c.SuplId) & (supl_intermedia.c.SuplFchAlta<=suplencias.c.SuplFchAlta)),
                 isouter=True) \
        ). \
        where( \
            (rlt.c.RelLabFchIniActividades >= sa.bindparam('inicioLectivo')) & \
            (rlt.c.PersonalPerId.in_(set_perids_novedades)) & \
            (puestos.c.PuestoFuncionId.in_(puestos_funcion)) & \
            (supl_intermedia.c.SuplId==None) & \
            (rlt.c.RelLabFchIniActividades < cast(suplencias.c.SuplFchAlta,Date)) \
        )
    df_primera_suplencia = pd.read_sql_query(query_primera_suplencia, engine, params=parametros)
    df_primera_suplencia.loc[:,'Origen']=['df_primera_suplencia']

    # (3) altas inyectadas en la bandeja para el período de licencia si es Junta Médica o Pase en Comisión
    query_alta_suplencia_paga = sa. \
        select([rlt.c.PersonalPerId,puestos.c.PuestoFuncionId,rlt.c.RelLabId,rlt.c.RelLabDesignCaracter,rlt.c.RelLabCicloPago,func.GREATEST(rlt.c.RelLabFchIniActividades,func.ADDDATE(cast(suplencias.c.SuplFchAlta,Date),1)).label('RelLabFchIniActividades'),func.IFNULL(rls.c.RelLabCeseFchReal,rlt.c.RelLabFchFinPrevista).label('RelLabCeseFchReal'),literal_column('50').label('CauBajCod'),silla_grupo_materia.c.GrupoMateriaId,sillas.c.TurnoId,sillas.c.SillaDependId,funciones_relacion_laboral.c.FuncRelLabCantHrs,sillas.c.FuncionId,rlt.c.RelLabAnulada,puestos.c.PuestoAsignId,asignaturas_materias.c.AsignId,suplencias.c.SuplCausId]). \
        select_from(
            jsupl.
            join(j3, ((rlt.c.RelLabId==suplencias.c.RelLabId) & (rlt.c.RelLabAnulada==0))) \
        ). \
        where( \
            (rlt.c.RelLabFchIniActividades >= sa.bindparam('inicioLectivo')) & \
            (rlt.c.PersonalPerId.in_(set_perids_novedades)) & \
            (puestos.c.PuestoFuncionId.in_(puestos_funcion)) & \
            (suplencias.c.SuplCausId.in_([16, 17, 162])) & \
            (func.GREATEST(rlt.c.RelLabFchIniActividades,func.ADDDATE(cast(suplencias.c.SuplFchAlta,Date),1)) <= func.IFNULL(rls.c.RelLabCeseFchReal,rlt.c.RelLabFchFinPrevista)) \
        )
    df_alta_suplencia_paga = pd.read_sql_query(query_alta_suplencia_paga, engine, params=parametros)
    df_alta_suplencia_paga.loc[:,'Origen']=['df_alta_suplencia_paga']

    # Las Juntas Médicas van con asignatura 162:
    df_alta_suplencia_paga.loc[df_alta_suplencia_paga['SuplCausId']==162,['AsignId','CauBajCod']] = [162, 66]
    # Los pases en comisión DENTRO ANEP van con dependencia 8902
    df_alta_suplencia_paga.loc[df_alta_suplencia_paga['SuplCausId']==16,['SillaDependId','CauBajCod']] = [8902, 66]
    # Los pases en comisión FUERA SECUN van con dependencia 8901
    df_alta_suplencia_paga.loc[df_alta_suplencia_paga['SuplCausId']==17,['SillaDependId','CauBajCod']] = [8901, 66]

    del df_alta_suplencia_paga['SuplCausId']

    df_historia_completa = pd.concat([df_historia_rl,df_primera_suplencia,df_alta_luego_de_suplencia,df_alta_suplencia_paga],axis=0)
    df_historia_completa = df_historia_completa.rename(columns = {'RelLabFchIniActividades':'falta','RelLabCeseFchReal':'fcese','SillaDependId':'dependid'})

    df_historia_completa = df_historia_completa.reset_index(drop=True)
    df_historia_completa.merge(df_anulaciones_periodo, on='RelLabId', how='left')
    df_anulaciones_a_eliminar = df_anulaciones_periodo[df_anulaciones_periodo['RelLabId'].isin(df_novedades['RelLabId'])]
    # Elimino los anulaciones de la historia
    df_historia_completa = df_historia_completa[df_historia_completa['RelLabId'].isin(df_anulaciones_a_eliminar['RelLabId'])==False]

    # obtengo los datos de las personas
    query_personas = sa.select([personas.c.PerId.label('PersonalPerId'),personas_documentos.c.PerDocId]).select_from(personas.join(personas_documentos)).where((personas_documentos.c.PaisCod=='UY')&(personas_documentos.c.DocCod=='CI')&(personas.c.PerId.in_(set_perids_novedades)))
    df_personas = pd.read_sql_query(query_personas, engine, params=parametros)
    df_historia_completa = df_historia_completa.merge(df_personas, on='PersonalPerId', how='left')

    # agrego asignatura 151 a todos los que no la tienen
    df_historia_completa.loc[((df_historia_completa['AsignId'].isnull()) & (df_historia_completa['PuestoAsignId'].notnull())),'AsignId']=df_historia_completa['PuestoAsignId']
    df_historia_completa.loc[(df_historia_completa['AsignId'].isnull()),'AsignId']=cfg['asignid_otros']

    df_historia_completa = df_historia_completa.loc[:,['PerDocId','dependid','AsignId','RelLabCicloPago','RelLabDesignCaracter','FuncRelLabCantHrs','falta','fcese','CauBajCod','GrupoMateriaId','FuncionId','RelLabAnulada','PersonalPerId','RelLabId']]

    # atributos hardcoded
    df_historia_completa['PerDocTpo']='DO'
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_hap.tolist()),'RelLabDesignCaracter']=cfg['caracter_horas_apoyo']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_hap.tolist()),'AsignId']=cfg['asignid_horas_apoyo']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_POB.tolist()),'RelLabDesignCaracter']=cfg['caracter_pob']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_POB.tolist()),'AsignId']=cfg['asignid_pob']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_POP.tolist()),'RelLabDesignCaracter']=cfg['caracter_pop']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_POP.tolist()),'AsignId']=cfg['asignid_pop']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_68.tolist()),'RelLabDesignCaracter']=cfg['caracter_68']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_68.tolist()),'AsignId']=cfg['asignid_68']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_talleristas.tolist()),'RelLabDesignCaracter']=cfg['caracter_talleristas']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_talleristas.tolist()),'AsignId']=cfg['asignid_talleristas']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_coordinadores_especiales.tolist()),'RelLabDesignCaracter']=cfg['caracter_especiales']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_coordinadores_especiales.tolist()),'AsignId']=cfg['asignid_especiales']
    df_historia_completa.loc[(df_historia_completa['AsignId']==75) & (df_historia_completa['fcese'].notnull()),'CauBajCod']=cfg['causal_coordinacion']
    df_historia_completa.loc[(df_historia_completa['RelLabAnulada']==1),'CauBajCod']=cfg['causal_anulacion']
    df_historia_completa['PerDocPaisCod']='UY'
    df_historia_completa['HorClaCurTpo']=''
    df_historia_completa['HorClaCur']=''
    df_historia_completa['HorClaArea']=''
    df_historia_completa['HorClaAnio']=0
    df_historia_completa['HorClaHorTope']=0
    df_historia_completa['HorClaObs']=''
    df_historia_completa['HorClaNumInt']=0
    df_historia_completa['HorClaParPreCod']=0
    df_historia_completa['HorClaCompPor']=0
    df_historia_completa['HorClaCompPor']=0
    df_historia_completa['HorClaLote']=0
    df_historia_completa['HorClaAudUsu']=0
    df_historia_completa['HorClaMod']=0
    df_historia_completa['HorClaEmpCod']=1
    df_historia_completa['HorClaCarNum']=0
    df_historia_completa['DesFchCarga']= date.today()
    df_historia_completa['Resultado']='PE'
    df_historia_completa['Mensaje']=''
    df_historia_completa['HorClaFchLib']=df_historia_completa['fcese']
    df_historia_completa.loc[(df_historia_completa['CauBajCod'].isnull()),'CauBajCod']=0

    del df_historia_completa['FuncionId']
    del df_historia_completa['PersonalPerId']

    #Transformacion de la dependencia a Siap
    df_historia_completa=df_historia_completa.merge(df_tabla_institucional)
    del df_historia_completa['dependid'] #borro la dependencia ya que voy a usar la dependidSiap

    # filtro los que tienen fcese < falta
    df_historia_completa = df_historia_completa.loc[(df_historia_completa['fcese']>=df_historia_completa['falta'])| (df_historia_completa['fcese'].isnull())]

    # filtro los que tienen falta >= tope
    df_historia_completa = df_historia_completa.loc[df_historia_completa['falta']<parametros['tope']]

    # filtro los que tienen cero horas
    df_historia_completa = df_historia_completa.loc[df_historia_completa['FuncRelLabCantHrs']>0]

    if ci!=None: # si me pasaron una ci como parametro filtro la historia solo para esa ci.
        df_historia_completa = df_historia_completa.loc[df_historia_completa['PerDocId']==ci]

    # Le pongo los nombres de los campos que corresponden a la tabla ihorasclase de siap
    df_historia_completa = df_historia_completa.rename(columns = {'PerDocId':'PerDocNum','RelLabDesignCaracter':'HorClaCar','RelLabCicloPago':'HorClaCic','falta':'HorClaFchPos','fcese':'HorClaFchCese','CauBajCod':'HorClaCauBajCod','GrupoMateriaId':'HorClaGrupo','dependidSiap':'HorClaInsCod','FuncRelLabCantHrs':'HorClaHor','AsignId':'HorClaAsiCod','RelLabAnulada':'HorClaBajLog'})

    df_historia_completa.to_sql(name='ihorasclase', con=engine_bandeja_out, if_exists= 'append', index=False)

if __name__=='__main__':
    main() 
    exit(0)
