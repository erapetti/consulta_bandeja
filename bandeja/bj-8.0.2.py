# coding: utf-8
#!/usr/bin/python
import sqlalchemy as sa
from sqlalchemy import create_engine, func
import pandas as pd
import pymysql
import datetime as dt
from datetime import date
import sys
import yaml
import numpy as np
import click

@click.command()
@click.option('--inicio', help = 'Fecha de inicio de las novedades')
@click.option('--fin', help = 'Fecha de fin de las novedades')
@click.option('--correcciones',default=0, help = 'correcciones = 1, graba solo lo que es novedades con actividad anterior al periodo')
@click.option('--ci',help = "ej : ci='33613521', considera los datos de salida solo para la cedula de identidad indicada")

def main(inicio,fin,correcciones,ci):

    """
    En el archivo bandejas.cfg se guardan los parametros de configuracion para la base de datos. El procedimiento toma el archivo de configuracion desde el mismo directorio donde se encuentra.
    \nEjecucion
    El procedimiento se ejecuta de la siguiente forma:
    (ejemplo)\n
    $python bj.py --inicio='2018-05-01' --fin='2018-05-14'

        - Novedades en el periodo [inicio, fin) , incluyendo inicio y no incluye la fecha de fin.Las novedades se refiere a las altas de designacion, ceses de designacion, cambios en las horas de coordinacion, anulaciones y pasajes a suplencias con reserva de cargo (SuplCausCod=6) .
        - Para todas las personas (PerId) que tuvieron novedades en el periodo indicado, se toman los datos de toda la historia de altas, ceses, horas de coordinacion, con tope el 01/03 del año correspondiente a la fecha de inicio que es pasada como parametro.
    """

    with open('bandejas.cfg', 'r') as ymlfile:
        cdb = yaml.load(ymlfile)

    with open('config.cfg', 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    engine = create_engine('mysql+pymysql://'+cdb['personal']['usr']+':'+cdb['personal']['password']+'@'+cdb['personal']['host']+'/'+cdb['personal']['bd'])
    engine_personas = create_engine('mysql+pymysql://'+cdb['personas']['usr']+':'+cdb['personas']['password']+'@'+cdb['personas']['host']+'/'+cdb['personas']['bd'])
    engine_estudiantil = create_engine('mysql+pymysql://'+cdb['estudiantil']['usr']+':'+cdb['estudiantil']['password']+'@'+cdb['estudiantil']['host']+'/'+cdb['estudiantil']['bd'])
    engine_bandeja = create_engine('mysql+pymysql://'+cdb['bandeja']['usr']+':'+cdb['bandeja']['password']+'@'+cdb['bandeja']['host']+'/'+cdb['bandeja']['bd'])


    # los puestos considerados docencia directa
    puestos_funcion = cfg['puestos_funcion']
    parametros = {}
    parametros['p1d']=dt.datetime(int(inicio.split('-')[0]),int(inicio.split('-')[1]),int(inicio.split('-')[2]))
    parametros['p2d']=dt.datetime(int(fin.split('-')[0]),int(fin.split('-')[1]),int(fin.split('-')[2]))
    # pInid es la fecha tope de inicio de actividades para tener en cuenta en la historia 
    parametros['pInid']=inicio.split('-')[0]+'-03-01'
    # pInis es la fecha tope desde que se consideran los pasajes a suplencias
    parametros['pInis']=cfg['inicio_suplencias']    #'2017-09-01'
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
    horas_coordinacion = sa.Table('HORAS_COORDINACION', metadata, autoload=True, autoload_with=engine)
    suplencias = sa.Table('SUPLENCIAS', metadata, autoload=True, autoload_with=engine)
    suplencias_causal = sa.Table('SUPLENCIAS_CAUSALES', metadata, autoload=True, autoload_with=engine)
    funciones_agrup_lin = sa.Table('FUNCION_AGRUP_LIN', metadata, autoload=True, autoload_with=engine)

    # cargo metadatos de Personas
    personas = sa.Table('PERSONAS', metadata, autoload=True, autoload_with=engine_personas)
    personas_documentos = sa.Table('PERSONASDOCUMENTOS', metadata, autoload=True, autoload_with=engine_personas)
    # cargo los datos de materias de estudiantil
    asignaturas_materias = sa.Table('ASIGNATURAS_MATERIAS', metadata, autoload=True, autoload_with=engine_estudiantil)

    # cargo las materias de estudiantil
    query_asignaturas_materias = sa.select([asignaturas_materias])
    df_asignaturas_materias = pd.read_sql_query(query_asignaturas_materias, engine_estudiantil, params=parametros)
    # cargo los datos de la base de siap para las dependencias
    tabla_institucional = sa.Table('tabla_institucional',metadata, autoload=True, autoload_with=engine_bandeja)
    suspensiones = sa.Table('suspensiones',metadata, autoload=True,autoload_with=engine_bandeja)
    query_tabla_institucional = sa.select([tabla_institucional])
    query_tabla_suspensiones = sa.select([suspensiones])
    df_tabla_institucional = pd.read_sql_query(query_tabla_institucional, engine_bandeja, params=parametros)
    df_suspensiones = pd.read_sql_query(query_tabla_suspensiones, engine_bandeja,params=parametros)


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
    query_altas_novedades = sa.select([relaciones_laborales.c.PersonalPerId, relaciones_laborales.c.RelLabId]).select_from(relaciones_laborales.join(puestos, relaciones_laborales.c.PuestoId==puestos.c.PuestoId)).where((relaciones_laborales.c.RelLabFchIniActividades >= sa.bindparam('p1d')) & (relaciones_laborales.c.RelLabFchIniActividades < sa.bindparam('p2d')) & (puestos.c.PuestoFuncionId.in_(puestos_funcion)))

    query_altas_novedades_cr = sa.select([relaciones_laborales.c.PersonalPerId, relaciones_laborales.c.RelLabId]).select_from(relaciones_laborales.join(puestos, relaciones_laborales.c.PuestoId==puestos.c.PuestoId)).where((relaciones_laborales.c.RelLabFchIniActividades < sa.bindparam('p1d')) & (relaciones_laborales.c.RelLabDesignFchAlta >= sa.bindparam('p1d')) & (relaciones_laborales.c.RelLabDesignFchAlta < sa.bindparam('p2d')) & (puestos.c.PuestoFuncionId.in_(puestos_funcion)))

    query_ceses_novedades = sa.select([relaciones_laborales.c.PersonalPerId, relaciones_laborales.c.RelLabId]).select_from(relaciones_laborales.join(puestos, relaciones_laborales.c.PuestoId==puestos.c.PuestoId)).where((relaciones_laborales.c.RelLabCeseFchReal >= sa.bindparam('p1d')) & (relaciones_laborales.c.RelLabCeseFchReal < sa.bindparam('p2d')) & (puestos.c.PuestoFuncionId.in_(puestos_funcion)))

    query_ceses_novedades_cr = sa.select([relaciones_laborales.c.PersonalPerId, relaciones_laborales.c.RelLabId]).select_from(relaciones_laborales.join(puestos, relaciones_laborales.c.PuestoId==puestos.c.PuestoId)).where((relaciones_laborales.c.RelLabCeseFchReal < sa.bindparam('p1d')) & (relaciones_laborales.c.RelLabCeseFchAlta >= sa.bindparam('p1d')) & (relaciones_laborales.c.RelLabCeseFchAlta < sa.bindparam('p2d')) & (puestos.c.PuestoFuncionId.in_(puestos_funcion)))

    df_novedades_alta = pd.read_sql_query(query_altas_novedades, engine, params=parametros)
    df_novedades_cese = pd.read_sql_query(query_ceses_novedades, engine, params=parametros)
    df_novedades_alta_cr = pd.read_sql_query(query_altas_novedades_cr, engine, params=parametros)
    df_novedades_cese_cr = pd.read_sql_query(query_ceses_novedades_cr, engine, params=parametros)
    df_novedades = pd.concat([df_novedades_alta,df_novedades_alta_cr,df_novedades_cese,df_novedades_cese_cr], axis=0)

    # elimino registros sin perid no se porque aparecen algunos NO deberian
    df_novedades = df_novedades.loc[df_novedades['PersonalPerId'].notnull()]

    # horas de coordinacion del periodo (novedades)
    query_hc_periodo = sa.select([horas_coordinacion]).where((horas_coordinacion.c.HrsCoordinacionFechaRegistro>= sa.bindparam('p1d')) & (horas_coordinacion.c.HrsCoordinacionFechaRegistro < sa.bindparam('p2d'))) 
    df_hc_periodo = pd.read_sql_query(query_hc_periodo, engine, params=parametros)
    # cargo las anulaciones del periodo
    query_anulaciones_periodo = sa.select([relaciones_laborales.c.PersonalPerId,relaciones_laborales.c.RelLabId, anulaciones.c.AnulacionFchAlta]).select_from(anulaciones.join(relaciones_laborales.join(puestos, relaciones_laborales.c.PuestoId==puestos.c.PuestoId), anulaciones.c.AnulacionValorPkTabla ==relaciones_laborales.c.RelLabId)).where((anulaciones.c.AnulacionFchAlta >= sa.bindparam('p1d')) & (anulaciones.c.AnulacionFchAlta < sa.bindparam('p2d'))&(anulaciones.c.AnulacionTipoNombre=='DESIGNACION') & (puestos.c.PuestoFuncionId.in_(puestos_funcion)))
    df_anulaciones_periodo = pd.read_sql(query_anulaciones_periodo,engine,params=parametros)

    query_suplencias = sa.select([relaciones_laborales.c.PersonalPerId,suplencias.c.RelLabId,suplencias.c.SuplFchAlta]).select_from(suplencias.join(relaciones_laborales.join(puestos, relaciones_laborales.c.PuestoId == puestos.c.PuestoId), suplencias.c.RelLabId==relaciones_laborales.c.RelLabId)).where((suplencias.c.SuplFchAlta >= sa.bindparam('pInis')) & (suplencias.c.SuplFchAlta < sa.bindparam('p2d')) & (puestos.c.PuestoFuncionId.in_(puestos_funcion)) & (suplencias.c.SuplCausId==6))# suplenciaId = 6 reserva de cargo
    df_suplencias = pd.read_sql_query(query_suplencias, engine, params=parametros)
    # cesesxretoma son ceses de RL(podrian estar vacantes) dentro del periodo tal que son suplentes de alguna otra relacion laboral.
    query_cesesxretoma_novedad = sa.select([relaciones_laborales.c.PersonalPerId,suplencias.c.RelLabId,relaciones_laborales.c.RelLabId,relaciones_laborales.c.RelLabCeseFchReal]).select_from(suplencias.join(relaciones_laborales.join(puestos, relaciones_laborales.c.PuestoId == puestos.c.PuestoId), suplencias.c.SuplRelLabId==relaciones_laborales.c.RelLabId)).where((puestos.c.PuestoFuncionId.in_(puestos_funcion))&(suplencias.c.SuplCausId==6)&(relaciones_laborales.c.RelLabCeseFchReal >= sa.bindparam('p1d')) & (relaciones_laborales.c.RelLabCeseFchReal < sa.bindparam('p2d'))&(relaciones_laborales.c.CauBajCod=='66')&(relaciones_laborales.c.RelLabAnulada==0))
    query_cesesxretoma_cr = sa.select([relaciones_laborales.c.PersonalPerId,suplencias.c.RelLabId,relaciones_laborales.c.RelLabId,relaciones_laborales.c.RelLabCeseFchReal]).select_from(suplencias.join(relaciones_laborales.join(puestos, relaciones_laborales.c.PuestoId == puestos.c.PuestoId), suplencias.c.SuplRelLabId==relaciones_laborales.c.RelLabId)).where((puestos.c.PuestoFuncionId.in_(puestos_funcion))&(suplencias.c.SuplCausId==6)&(relaciones_laborales.c.RelLabCeseFchReal < sa.bindparam('p1d')) & (relaciones_laborales.c.RelLabCeseFchAlta >= sa.bindparam('p1d')&(relaciones_laborales.c.RelLabCeseFchAlta >= sa.bindparam('p1d')))&(relaciones_laborales.c.CauBajCod=='66')&(relaciones_laborales.c.RelLabAnulada==0))
    df_cesesxretoma_novedad= pd.read_sql_query(query_cesesxretoma_novedad, engine, params=parametros)
    df_cesesxretoma_cr= pd.read_sql_query(query_cesesxretoma_cr, engine, params=parametros)
    df_cesesxretoma_novedad.columns = ['peridS','RelLabIdT','RelLabIdS','RelLabFchCeseRealS']
    df_cesesxretoma_cr.columns = ['peridS','RelLabIdT','RelLabIdS','RelLabFchCeseRealS']
    df_cesesxretoma = pd.concat([df_cesesxretoma_novedad,df_cesesxretoma_cr],axis=0)
    #

    # perids
    set_perids_novedades = (df_novedades['PersonalPerId'].append(df_hc_periodo['CoordPerId'])).append(df_anulaciones_periodo['PersonalPerId']).append(df_suplencias['PersonalPerId']).unique().tolist()

    # obtengo los datos de las personas
    query_personas = sa.select([personas.c.PerId, personas.c.PerNombreCompleto,personas_documentos.c.PerDocId]).select_from(personas.join(personas_documentos, personas.c.PerId == personas_documentos.c.PerId)).where((personas_documentos.c.PaisCod=='UY')&(personas_documentos.c.DocCod=='CI')&(personas.c.PerId.in_((set_perids_novedades))))
    df_personas = pd.read_sql_query(query_personas, engine_personas, params=parametros)
    df_personas = df_personas.rename(columns= {'PerId':'perid', 'PerNombreCompleto':'nombre','PerDocId':'ci'})
    ## Tomo la historia de los perid con novedades
    # join
    j3 = (relaciones_laborales.join(funciones_relacion_laboral.join(funciones_asignadas.join(sillas.join(silla_grupo_materia, sillas.c.SillaId==silla_grupo_materia.c.SillaId, isouter=True), sillas.c.SillaId==funciones_asignadas.c.SillaId), funciones_asignadas.c.FuncAsignadaId==funciones_relacion_laboral.c.FuncAsignadaId), relaciones_laborales.c.RelLabId==funciones_relacion_laboral.c.RelLabId).join(puestos))
    query_historia_rl = sa.select([relaciones_laborales.c.PersonalPerId, puestos.c.PuestoFuncionId,relaciones_laborales.c.RelLabId,relaciones_laborales.c.RelLabDesignCaracter,relaciones_laborales.c.RelLabCicloPago,relaciones_laborales.c.RelLabFchIniActividades, relaciones_laborales.c.RelLabCeseFchReal, relaciones_laborales.c.CauBajCod,silla_grupo_materia.c.GrupoMateriaId,sillas.c.MateriaId,sillas.c.TurnoId, sillas.c.SillaDependId,funciones_relacion_laboral.c.FuncRelLabCantHrs,sillas.c.FuncionId,relaciones_laborales.c.RelLabAnulada]).select_from(j3).where((relaciones_laborales.c.RelLabFchIniActividades >= sa.bindparam('pInid'))& relaciones_laborales.c.PersonalPerId.in_(set_perids_novedades) & (puestos.c.PuestoFuncionId.in_(puestos_funcion)))
    df_historia_rl = pd.read_sql_query(query_historia_rl, engine, params=parametros)
    #Altas de generadas por ceses de retoma de titular.
    hayaltasxretomatitulares=False 
    if len(df_cesesxretoma['RelLabIdT'].unique().tolist()) > 0: #si hay algun cese por retoma (evito warning performance)
        query_altasxretoma_titulares = sa.select([relaciones_laborales.c.RelLabId,funciones_relacion_laboral.c.FuncAsignadaId,relaciones_laborales.c.PersonalPerId, puestos.c.PuestoFuncionId,relaciones_laborales.c.RelLabId,relaciones_laborales.c.RelLabDesignCaracter,relaciones_laborales.c.RelLabCicloPago,relaciones_laborales.c.RelLabFchIniActividades, relaciones_laborales.c.RelLabCeseFchReal, relaciones_laborales.c.CauBajCod,silla_grupo_materia.c.GrupoMateriaId,sillas.c.MateriaId,sillas.c.TurnoId, sillas.c.SillaDependId,funciones_relacion_laboral.c.FuncRelLabCantHrs,sillas.c.FuncionId,relaciones_laborales.c.RelLabAnulada,relaciones_laborales.c.MigraDD_Curso]).select_from(j3).where((relaciones_laborales.c.RelLabFchIniActividades >= sa.bindparam('pInid'))&(relaciones_laborales.c.RelLabId.in_(df_cesesxretoma['RelLabIdT'].unique().tolist()))&(puestos.c.PuestoFuncionId.in_(puestos_funcion)))
        df_altasxretoma_titulares= pd.read_sql_query(query_altasxretoma_titulares, engine, params=parametros)
        df_altasxretoma_titulares= df_altasxretoma_titulares.merge(df_asignaturas_materias,on='MateriaId', how='left')
        df_altasxretoma_titulares = df_altasxretoma_titulares.rename(columns = {'PersonalPerId':'perid','RelLabDesignCaracter':'caracter','RelLabCicloPago':'ciclo','RelLabFchIniActividades':'falta','RelLabCeseFchReal':'fcese','CauBajCod':'causal','GrupoMateriaId':'grupo','TurnoId':'turno','SillaDependId':'dependid','FuncRelLabCantHrs':'horas','AsignId':'asignid','RelLabAnulada':'anulada'})
        for ix, row in df_cesesxretoma.iterrows():
          df_altasxretoma_titulares.loc[df_altasxretoma_titulares['RelLabId']==row['RelLabIdT'],['falta']]=row['RelLabFchCeseRealS']
        hayaltasxretomatitulares = True
    ## le agrego la asignid a df_historia_rl 
    df_historia_rl = df_historia_rl.merge(df_asignaturas_materias,on='MateriaId', how='left')
    df_historia_rl = df_historia_rl.rename(columns = {'PersonalPerId':'perid','RelLabDesignCaracter':'caracter','RelLabCicloPago':'ciclo','RelLabFchIniActividades':'falta','RelLabCeseFchReal':'fcese','CauBajCod':'causal','GrupoMateriaId':'grupo','TurnoId':'turno','SillaDependId':'dependid','FuncRelLabCantHrs':'horas','AsignId':'asignid','RelLabAnulada':'anulada'})
    del df_historia_rl['MateriaId']

    for ix, row in df_suplencias.iterrows():
        df_historia_rl.loc[df_historia_rl['RelLabId']==row['RelLabId'],['fcese']]=row['SuplFchAlta'].date()

    query_hc_hist = sa.select([horas_coordinacion]).where((horas_coordinacion.c.HrsCoordinacionFechaAlta >= sa.bindparam('pInid')) & (horas_coordinacion.c.CoordPerId.in_(set_perids_novedades))) 
    df_historia_hc = pd.read_sql_query(query_hc_hist, engine, params=parametros)

    df_historia_hc['asignid']='75'
    df_historia_hc['caracter']='I'

    # Transformacion de horas coordinacion
    def coord_to_alta(row):
       if row['HrsCoordConSigno'] > 0:
           val = row['HrsCoordinacionFechaAlta']
       else: #si es menor que cero entonces es un cese por lo que se pone en falta la fecha del ultimo alta que tuvo
           val = row['HrsCoordFchUltAlta']
       return val

    def coord_to_cese(row):
       if row['HrsCoordConSigno'] < 0:
           val = row['HrsCoordinacionFechaAlta']
       else:
           val = np.NaN
       return val

    if not df_historia_hc.empty:
        df_historia_hc['falta'] = df_historia_hc.apply(coord_to_alta,axis=1)
        df_historia_hc['fcese'] = df_historia_hc.apply(coord_to_cese,axis=1)
        df_historia_hc['horas']=np.abs(df_historia_hc['HrsCoordConSigno'])
        df_historia_hc = df_historia_hc.rename(columns = {'DependCoordId':'dependid'})
        df_historia_hc = df_historia_hc.rename(columns = {'CoordPerId':'perid'})
        df_historia_hc = df_historia_hc.rename(columns = {'CicloDePago':'ciclo'})
        # tomo los ids de altas de horas de coordinacion que no van porque tienen cese
        idsAltas=df_historia_hc.loc[df_historia_hc['HrsCoordinacionIdAlta']>0,'HrsCoordinacionIdAlta'].tolist()
        df_historia_hc = df_historia_hc[df_historia_hc['HrsCoordinacionId'].isin(idsAltas)==False]
        df_historia_hc = df_historia_hc.loc[:,['perid','dependid','asignid','ciclo','caracter','horas','falta','fcese']]

    if  hayaltasxretomatitulares: 
        df_historia_completa = pd.concat([df_historia_hc,df_historia_rl,df_altasxretoma_titulares], axis=0)
    else:
        df_historia_completa = pd.concat([df_historia_hc,df_historia_rl], axis=0)
    df_historia_completa = df_historia_completa.reset_index(drop=True)
    df_historia_completa = df_historia_completa.merge(df_personas, on='perid', how='left')
    df_historia_completa.merge(df_anulaciones_periodo, on='RelLabId', how='left')
    df_anulaciones_a_eliminar = df_anulaciones_periodo[df_anulaciones_periodo['RelLabId'].isin(df_novedades['RelLabId'])]
    # Elimino los anulaciones de la historia
    df_historia_completa = df_historia_completa[df_historia_completa['RelLabId'].isin(df_anulaciones_a_eliminar['RelLabId'])==False]
    df_anulaciones_a_generar = df_anulaciones_periodo[df_anulaciones_periodo['RelLabId'].isin(df_novedades['RelLabId'])==False]
    df_anulaciones_periodo = df_anulaciones_periodo.rename(columns={'PersonalPerId':'perid'})

    df_historia_completa = df_historia_completa.loc[:,['ci','nombre','dependid','asignid','ciclo','caracter','horas','turno','falta','fcese','causal','grupo','FuncionId','anulada','perid']]
    # atributos harcoded
    df_historia_completa['PerDocTpo']='DO'
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_hap.tolist()),'caracter']=cfg['caracter_horas_apoyo']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_hap.tolist()),'asignid']=cfg['asignid_horas_apoyo']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_POB.tolist()),'caracter']=cfg['caracter_pob']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_POB.tolist()),'asignid']=cfg['asignid_pob']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_POP.tolist()),'caracter']=cfg['caracter_pop']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_POP.tolist()),'asignid']=cfg['asignid_pop']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_68.tolist()),'caracter']=cfg['caracter_68']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_68.tolist()),'asignid']=cfg['asignid_68']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_talleristas.tolist()),'caracter']=cfg['caracter_talleristas']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_funciones_talleristas.tolist()),'asignid']=cfg['asignid_talleristas']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_coordinadores_especiales.tolist()),'caracter']=cfg['caracter_especiales']
    df_historia_completa.loc[df_historia_completa['FuncionId'].isin(df_coordinadores_especiales.tolist()),'asignid']=cfg['asignid_especiales']
    df_historia_completa.loc[(df_historia_completa['asignid']=='75') & (df_historia_completa['fcese'].notnull()),'causal']=cfg['causal_coordinacion']
    df_historia_completa.loc[(df_historia_completa['anulada']==1),'causal']=cfg['causal_anulacion']
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
    df_historia_completa['Resultado']=''
    df_historia_completa['Mensaje']=''
    df_historia_completa['HorClaFchLib']=df_historia_completa['fcese']
    df_historia_completa['causal']=df_historia_completa['causal'].fillna(0)
    # agrego asignatura 81 a todos los que no la tienen
    df_historia_completa.loc[df_historia_completa['asignid'].isnull(),'asignid']=cfg['asignid_otros']
    del df_historia_completa['FuncionId']
    del df_historia_completa['nombre']
    del df_historia_completa['turno']
    #Transformacion de la dependencia a Siap
    df_tabla_institucional=df_tabla_institucional.rename(columns={'DEP_AS400':'dependid','DEP_DBC':'dependidSiap'})
    del df_tabla_institucional['DESCR_AS400']
    del df_tabla_institucional['DESCR_DBC']
    del df_tabla_institucional['ID']
    df_historia_completa=df_historia_completa.merge(df_tabla_institucional)
    del df_historia_completa['dependid'] #borro la dependencia ya que voy a usar la dependidSiap
    # filtro los que tienen fcese < falta
    df_historia_completa = df_historia_completa.loc[(df_historia_completa['fcese']>=df_historia_completa['falta'])| (df_historia_completa['fcese'].isnull())]
    if correcciones==1:
        df_historia_completa = df_historia_completa.loc[df_historia_completa['perid'].isin(df_novedades_alta_cr['PersonalPerId'])|df_historia_completa['perid'].isin(df_novedades_cese_cr['PersonalPerId'])]

    del df_historia_completa['perid']
    # Le pongo los nombres de los campos que corresponden a la tabla ihorasclase de siap
    df_historia_completa = df_historia_completa.rename(columns = {'ci':'PerDocNum','caracter':'HorClaCar','ciclo':'HorClaCic','falta':'HorClaFchPos','fcese':'HorClaFchCese','causal':'HorClaCauBajCod','grupo':'HorClaGrupo','dependidSiap':'HorClaInsCod','horas':'HorClaHor','asignid':'HorClaAsiCod','anulada':'HorClaBajLog'})
    # todas las horas de coordinacion con fecha de cese NULL les pongo 28/02/año siguiente
    df_historia_completa.loc[(df_historia_completa['HorClaAsiCod']=='75')&(df_historia_completa['HorClaFchCese'].isnull()),['HorClaFchCese']]=date(cfg['cese_hc_anio'],cfg['cese_hc_mes'],cfg['cese_hc_dia'])
    #Agrego la materia 162 para las suspensiones
    df_suspensiones = df_suspensiones[df_suspensiones['motivo']=='Junta M\xe9dica']
    df_historia_completa.loc[df_historia_completa['PerDocNum'].isin(df_suspensiones['PerDocNum']),'HorClaAsiCod']='162'
    if ci<>None: # si me pasaron una ci como parametro filtro la historia solo para esa ci.
        df_historia_completa = df_historia_completa.loc[df_historia_completa['PerDocNum']==ci]

    df_historia_completa.to_sql(name='ihorasclase', con=engine_bandeja, if_exists= 'append', index=False)

if __name__=='__main__':
    main() 
