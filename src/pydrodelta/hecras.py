import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import os

# from pyras.controllers import RAS41, kill_ras
# from pyras.controllers.hecras import ras_constants as RC

''' Instalar controlador:
pip install pyras --upgrade
pip install pywin32

Step 2: Run makepy utilities
- Go to the path where Python modules are sitting:
	It may look like this -> C:/Users\solo\Anaconda\Lib\site-packages\win32com\client
	or C:/Python27\ArcGIS10.2\Lib\site-packages\win32com\client
	or C:/Python27\Lib\site-packages\win32com\client
- Open command line at the above (^) path and run $: python makepy.py
	select HECRAS River Analysis System (1.1) from the pop-up window
	this will build definitions and import modules of RAS-Controller for use  '''

class ModelConfig():
    def __init__(self,params):
        self.geometry_file = params["geometry_file"]
        self.plan_file = params["plan_file"]
        self.unsteady_file = params["unsteady_file"]

def createHecRasProcedure(procedure,params,plan=None):
    procedure.workspace = params["workspace"]
    procedure.model_path = params["model_path"]
    if params["initial_load"]:
        procedure.initial_load = True
        procedure.loadTopologyFromModel()
    else:
        procedure.initial_load = False
        # self.loadTopology()
    procedure.project_name = params["project_name"]
    if "model_config" in params:
        procedure.model_config = ModelConfig(params["model_config"])
    procedure.loadTopologyFromModel = lambda : loadTopologyFromModel(procedure)
    procedure.run = lambda inline=True : run(procedure,inline)

def run(procedure,inline=True):
    """run procedure. Produces pivoted dataframe of output series with index of located datetimes, column names of node id's and values as floats.
    
    :param inline: save output inline (self.output). Default: True
    :type inline: bool
    :returns: procedure output result as dataframe if inline=False, else None
    """
    ListaCB = [] # pd.read_csv(self.workspace+'1_Lista_CB.csv',sep=',')

    # df_SeccSalidas = pd.read_csv(self.workspace+'2_Lista_Salidas.csv',sep=',')
    # df_SeccSalidas['River_Stat'] = df_SeccSalidas['River_Stat'].astype(int)

    procedure.loadInput()
    df_base = procedure.input.copy()
    
    # df_base = pd.DataFrame()
    for node in procedure.input_nodes: #df_ListaCB.iterrows():
        print("node %s" % str(node.id))
    #     df_i = node.concatenateProno(inline=False) # cargaSeries(node['series_id'],f_inicio_0,f_fin_0,serieID_prono=node['series_simulada'])
        
    #     df_i = df_i.rename(columns={'valor':node.id}) #,'Tipo':'Tipo'+str(node['FID'])})
    #     df_base = df_base.join(df_i, how = 'outer')
        cb = node.hec_node.copy()
        cb["FID"] = node.id
        cb["name"] = node.name
        ListaCB.append(cb)
            
    project_name = procedure.project_name
    CurrentUnSteadyFile = procedure.model_config.unsteady_file
    CurrentPlanFile = procedure.model_config.plan_file

    f_inicio = df_base.index[0]
    f_fin  = df_base.index[-1]

    # EDITA ARCHIVO DE CONDICIONES DE BORDE (.u)
    EditConBorde(procedure.model_path,project_name,CurrentUnSteadyFile,pd.DataFrame(ListaCB),f_inicio,f_fin,df_base)  # EDITA ARCHIVO DE CONDICIONES DE BORDE (.u)

    f_condBorde = f_inicio + timedelta(days=5)
    EditaPlan(procedure.model_path,project_name,CurrentPlanFile,f_inicio,f_fin,f_condBorde)

    print ('HEC-RAS:')
    hec_model = AbreModelo(procedure.model_path,project_name)
    correModelo(hec_model)

    ################################################################################################
    SeccSalidas = []
    for node in procedure.output_nodes:
        ss = node.hec_node.copy()
        ss["FID"] = node.id
        ss["name"] = node.name
        SeccSalidas.append(ss)

    output , list_1 = ExtraeSimulados_aDFyDic(hec_model,pd.DataFrame(SeccSalidas),plotear=False,pivot=True)
    output = output.reset_index(drop=True)

    # Cierra el modelo
    hec_model.close()

    # Guarda en CSV
    output.to_csv(procedure.workspace+'Salidas.csv', index=False, sep=',')

    if inline:
        procedure.output = output
    else:
        return output

def loadTopologyFromModel(procedure):
    procedure.model_config = {}
    procedure.model_config['project_name'] = procedure.project_name
    ## Abre Modelo
    rc_f = AbreModelo(procedure.model_path,procedure.project_name)
    ## Current Files
    GeomFile, PlanFile, UnSteadyFile = InfoModelo(rc_f)
    procedure.model_config['geometry_file'] = GeomFile
    procedure.model_config['plan_file'] = PlanFile
    procedure.model_config['unsteady_file'] = UnSteadyFile
    ## Lee Condicion de Borde del .u
    procedure.boundary_conditions = LeeCB(procedure.model_path,procedure.project_name,procedure.unsteady_file)
    # print(df_ListaCB)
    ## Lee Puntos de salida en el plan.
    procedure.output_sections = SalidasPaln(procedure.model.path,procedure.project_name,procedure.plan_file)
    # print(self.output_sections)
    rc_f.close()
    procedure.boundary_conditions.to_csv(procedure.workspace+'/1_Lista_CB_0.csv',sep=',',index=False)
    procedure.output_sections.to_csv(procedure.workspace+'/2_Lista_Salidas_0.csv',sep=',',index=False)
    ##  Escribe Archivo config
    # with open(workspace+'/config_Modelo.json', 'w') as fp:
    #     json.dump(self.model_config, fp)
    # fp.close()
    # quit()


# Abrel el modelo
def AbreModelo(rutaModelo,nombre_project):
    project = rutaModelo+'/'+nombre_project+'.prj'
    
    rc = RAS41()
    
    res = rc.HECRASVersion()
    print('\n	HECRASVersion: '+ res)
    rc.ShowRas()
    rc.Project_Open(project)
    return rc

# Imprime informacion del modelo
def InfoModelo(rc_f):
    res = rc_f.CurrentProjectTitle()
    ProjectTitle = res
    print('	CurrentProjectTitle: '+ ProjectTitle)
    res = rc_f.CurrentGeomFile()
    CurrentGeomFile = res.split('.')[-1]
    print('	CurrentGeomFile: '+ CurrentGeomFile)
    res1 = rc_f.CurrentPlanFile()
    CurrentPlanFile = res1.split('.')[-1]
    print('	CurrentPlanFile: '+ CurrentPlanFile)
    res = rc_f.CurrentUnSteadyFile()
    CurrentUnSteadyFile = res.split('.')[-1]
    print('	CurrentUnSteadyFile: '+ CurrentUnSteadyFile)
    return CurrentGeomFile, CurrentPlanFile, CurrentUnSteadyFile

# Consulta River, Reach, Node
def listToDict(lst):
    op = { i+1 : lst[i].strip() for i in range(0, len(lst) ) }
    return op

def InfoGeon(rc_g):
    # Geometry_GetRivers'
    C_Rivers_m = rc_g.Geometry_GetRivers()
    Rivers_m_l = C_Rivers_m[1]
    CantR = C_Rivers_m[0]
    # print(Rivers_m)
    geo = rc_g.Geometry()
    gres = geo.nRiver()
    print('nRiver')
    print(gres)
    print('')
    #quit()


    Rivers_m = listToDict(Rivers_m_l)
    for River_m in Rivers_m:
        id_Ri = River_m
        nombreRio = Rivers_m[River_m]
        print(id_Ri,': ',nombreRio)
        # Geometry_GetReaches
        C_Reaches_m = rc_g.Geometry_GetReaches(id_Ri)
        Reaches_m_l = C_Reaches_m[1]
        CantRe = C_Reaches_m[0]
        
        Reaches_m = listToDict(Reaches_m_l)
        
        for Reache_m in Reaches_m:
            id_Re = Reache_m
            nombre_Reach = Rivers_m[River_m]
            print(id_Re,': ',nombre_Reach)
            # Geometry_GetNodes
            C_Nodes_m = rc_g.Geometry_GetNodes(id_Ri, id_Re)
            Nodes_m_l = C_Nodes_m[0]
            
            #res = rc_g.Geometry_GetNode(1, 1, '5.39')
            #print('Geometry_GetNode')
            #print(res)
            #print('')
            print (Nodes_m_l)

def LeeCB(rutaModelo,nombre_project,CurrentUnSteadyFile):
# Lee Condicion de Borde del .u
    print ('\n Condiciones de Borde: \n')
    ruta_ptU = rutaModelo+'/'+nombre_project+'.'+CurrentUnSteadyFile
    f_ptU = open(ruta_ptU,'r')							#Abre el plan para leerlo
    df_ListaCB =  pd.DataFrame(columns=['FID', 'River','Reach', 'River_Stat','Interval','CondBorde'])
    id = 0
    for line in f_ptU:									#Lee linea por linea el plan
        line = line.rstrip()
        if line.startswith('Boundary Location='):
            line_c = line.split('=')[1].split(',')
            River, Reach, River_Stat = line_c[0].strip(),line_c[1].strip(),line_c[2].strip()
        if line.startswith('Interval='):
            Interval = line.split('=')[1].strip()
        if line.startswith('Stage Hydrograph='):
            tipoCondicion = line.split('=')[0].strip()
        if line.startswith('Flow Hydrograph='):
            tipoCondicion = line.split('=')[0].strip()
        if line.startswith('Lateral Inflow Hydrograph='):
            tipoCondicion = line.split('=')[0].strip()
        if line.startswith('DSS Path='):
            id += 1
            df_ListaCB = df_ListaCB.append({'FID': int(id), 
                                                    'River': River,
                                                    'Reach': Reach,
                                                    'River_Stat': River_Stat,
                                                    'Interval' : Interval,
                                                    'CondBorde' : tipoCondicion }, ignore_index=True)
    f_ptU.close()   #Cierra el archivo .u
    df_ListaCB['name'] = np.nan
    df_ListaCB['series_id'] = np.nan
    df_ListaCB['series_prono_id'] = np.nan
    df_ListaCB['cero'] = np.nan
    df_ListaCB['qualifier'] = np.nan
    return df_ListaCB

# EDITA ARCHIVO DE CONDICIONES DE BORDE (.u)

# Escribe el Archivo de CB .u
def EditConBorde(rutaModelo,nombre_project,CurrentUnSteadyFile,ListaCB,f_inicio,f_fin,df_series):  #FlowTitle,HEC_Vers,restNum,restNum2
    print ('Modifica condicion de borde(.u)')
    ruta_ptU = rutaModelo+'/'+nombre_project+'.'+CurrentUnSteadyFile    #Ruta del archivo (.u) de condiciones de borde
    FlowTitle = None
    HEC_Vers = None
    RestCond = None
    restFile = None
    f_ptU0 = open(ruta_ptU,'r')    #Abre el plan para leerlo
    for line in f_ptU0:										#Lee linea por linea el plan
        line = line.rstrip()
        if line.startswith('Flow Title='):				#Modifica la fecha de simulacion
            FlowTitle = line.split('=')[1]
        if line.startswith('Program Version='):				#Modifica la fecha de simulacion
            HEC_Vers = line.split('=')[1]
        if line.startswith('Use Restart='):				#Modifica la fecha de simulacion
            RestCond = line.split('=')[1]
        if line.startswith('Restart Filename='):				#Modifica la fecha de simulacion
            restFile = line.split('=')[1]
    f_ptU0.close()
    
    # Escribe el .u 
    f_ptU = open(ruta_ptU,"w")
    f_ptU.write('Flow Title='+FlowTitle+'\n')					#Escribe el titulo
    f_ptU.write('Program Version='+HEC_Vers+'\n')         #Version del programa
    #Condicion Inicial
    f_ptU.write('Use Restart='+RestCond+'\n')		#Usa una condicion inicial ya generada
    f_ptU.write('Restart Filename='+restFile+'\n')                         #Escribe el nombre del archivo de arranque utilizado
    f_ptU.write('Keep SA Constant=-1\n')
    
    # Funcion que esribe en el archivo .u (Unsteady Flow Data) cada condicion de borde
    def cargaDatos(rio, reach, id_prog, condborde, listQ, StartDate, Interval):
        f_ptU.write('Boundary Location='+rio+','+reach+','+str(id_prog)+',        ,                ,                ,                ,                \n')
        f_ptU.write('Interval='+Interval+'\n')
        f_ptU.write(condborde+'= '+str(len(listQ))+'\n')
        i2 = 1
        for i in listQ:
            if i2 == 10:
                f_ptU.write(str(i).rjust(8, ' ')+'\n')
                i2 = 1
            else:
                f_ptU.write(str(i).rjust(8, ' '))
                i2 += 1
        f_ptU.write('\n')
        
        if rio == 'Victoria':
            f_ptU.write('Flow Hydrograph Inital WS= 2\n')
        
        f_ptU.write('DSS Path=\n')
        f_ptU.write('Use DSS=False\n')
        f_ptU.write('Use Fixed Start Time=True\n')
        f_ptU.write('Fixed Start Date/Time='+StartDate+',\n')
        f_ptU.write('Is Critical Boundary=False\n')
        f_ptU.write('Critical Boundary Flow=\n')
	
    for idx,rios in ListaCB.iterrows():										#Loop para cada rio/reach con condicion de borde	
        rio = rios['River']												    	#Rio
        reach = rios['Reach']													#Reach
        print(rio, reach)
        id_prog = rios['River_Stat']												#Progresiva 
        condborde = rios['CondBorde']												#Tipo de condicion de borde
        f_inicio_Hdin = f_inicio.strftime('%d%b%Y')						#Fecha de inicion del modelo hidrodinamico
        id = rios['FID']													# node.id
        Interval = rios['Interval']
        
        dfh = df_series[id].copy()
        if  Interval == '1HOUR':
            dfh = dfh.resample('H').mean()
        if  Interval == '1DAY':
            dfh = dfh.resample('D').mean()								#Toma solo un dato diario, el prodio de los que tenga
        dfh = dfh.interpolate()
        listV = dfh.values.tolist()					#Pasa la columna a una lista
        listV = [ '%.2f' % elem for elem in listV ]					#Elimina decimales OJO!!!
		
		#Funcion Carga Datos: Escribe en el .u
        cargaDatos(rio, reach, id_prog, condborde, listV, f_inicio_Hdin, Interval)

    f_ptU.close()		#Cierra el archivo .u
    print ('Guarda condicion de borde(.u) \n')

# Lee Puntos de salida en el plan.
def SalidasPaln(rutaModelo,nombre_project,CurrentPlanFile):
    print ('\n Lee puntos de Salidas')
    ruta_plan = rutaModelo+'/'+nombre_project+'.'+CurrentPlanFile
    f_plan = open(ruta_plan,'r')							#Abre el plan para leerlo
    df_SalidasPlan =  pd.DataFrame(columns=['FID', 'River','Reach', 'River_Stat'])
    id = 0
    for line in f_plan:										#Lee linea por linea el plan
        line = line.rstrip()
        if line.startswith('Stage Flow Hydrograph='):
            line = line.split('=')[1]
            line = line.split(',')
            River, Reach, River_Stat = line[0].strip(),line[1].strip(),line[2].strip()
            id += 1
            df_SalidasPlan = df_SalidasPlan.append({'FID': int(id), 'River': River,'Reach': Reach,'River_Stat': River_Stat}, ignore_index=True)
            df_SalidasPlan['Node_Name'] = np.nan
            df_SalidasPlan['S_id_geom'] = np.nan
            df_SalidasPlan['S_id_hidro'] = np.nan
            df_SalidasPlan['S_id_obs'] = np.nan
            df_SalidasPlan['cero_escala'] = np.nan
    f_plan.close()
    return df_SalidasPlan

# Edita el Plan de corrida
def EditaPlan(rutaModelo,nombre_project,CurrentPlanFile,f_inicio,f_fin,f_condBorde):
	print ('Modifica el plan de la corrida')
	#Cambia el formato de las fechas para escribirlas en el plan del HECRAS
	f_inicio_Hdin = f_inicio.strftime('%d%b%Y')
	f_fin_Hdin = f_fin.strftime('%d%b%Y')
	f_condBorde = f_condBorde.strftime('%d%b%Y')

	ruta_plan = rutaModelo+'/'+nombre_project+'.'+CurrentPlanFile
	ruta_temp = rutaModelo+'/temp.'+CurrentPlanFile
	f_plan = open(ruta_plan,'r')							#Abre el plan para leerlo
	temp = open(ruta_temp,'w')								#Crea un archivo temporal
	for line in f_plan:										#Lee linea por linea el plan
		line = line.rstrip()
		if line.startswith('Simulation Date'):				#Modifica la fecha de simulacion
			newL1 = ('Simulation Date='+f_inicio_Hdin+',0000,'+f_fin_Hdin+',0000')
			temp.write(newL1+'\n')							#Escribe en el archivo temporal la fecha cambiada
		elif line.startswith('IC Time'):
			newL2 = ('IC Time=,'+f_condBorde+',')			#Modifica la fecha de condicion de borde
			temp.write(newL2+'\n')							#Escribe en el archivo temporal la fecha de condicon de borde
		else:
			temp.write(line+'\n')							#Escribe en el archivo temporal la misma linea
	temp.close()
	f_plan.close()
	os.remove(ruta_plan)									#Elimina el plan viejo
	os.rename(ruta_temp,ruta_plan)							#Cambia el nombre del archivo temporal
	print ('Guarda el plan de la corrida\n')

# Corre el HECRAS
def correModelo(rc_m):
	print('Corre el modelo:')
	res = rc_m.Compute_CurrentPlan()				#Corre el modelo
	if res == False:
		print('	Error al correr el modelo.')
		rc_m.close()
		kill_ras()
		quit()
	if res == True:
		print('	Sin problemas')

# Loop sobre el archivo con las secciones de salida
def ExtraeSimulados_aDF(hec_model,Estaciones,plotear=False):
    df_SSalidas = pd.DataFrame()
    for index, Estac in Estaciones.T.iteritems():
        Id = Estac['FID']
        river = Estac['River']
        reach = Estac['Reach']
        RS = Estac['River_Stat']
        nombre = Estac['Node_Name']
        
        print (Id,' ',river,' - ',reach,': ',RS)
        print (nombre)

        #CARGA SIMULADOS
        res = hec_model.OutputDSS_GetStageFlow(river, reach, RS)
        res = list(res)
        data = pd.DataFrame({'fecha': res[1], 'nivel_sim': res[2], 'caudal_sim': res[3]})
        data['fecha'] =  pd.to_datetime(data['fecha'])
        data['Id'] = int(Id)
        data['nivel_sim'] = data['nivel_sim'].round(3)
        data['caudal_sim'] = data['caudal_sim'].round(3)

        df_SSalidas = pd.concat([df_SSalidas, data], ignore_index=False)
        if plotear == True:
            plt.plot(data['nivel_sim'],label=nombre)
            plt.xlabel('Fecha')
            plt.ylabel('Altura')
            #plt.title(CBi['NomBBDD'])
            plt.legend()
            plt.show()
            plt.close()
    return df_SSalidas


def ExtraeSimulados_aDFyDic(hec_model,Estaciones,plotear=False,pivot=True):
    df_SSalidas = pd.DataFrame()
    #list_1 = []
    for index, Estac in Estaciones.T.iteritems():
        Id = Estac['FID']
        river = Estac['River']
        reach = Estac['Reach']
        RS = Estac['River_Stat']
        nombre = Estac['Node_Name']
        
        print (Id,' ',river,' - ',reach,': ',RS)
        print (nombre)

        #CARGA SIMULADOS
        res = hec_model.OutputDSS_GetStageFlow(river, reach, RS)
        res = list(res)
        data = pd.DataFrame({'fecha': res[1], 'nivel_sim': res[2], 'caudal_sim': res[3]})
        data['fecha'] =  pd.to_datetime(data['fecha'])
        data['Id'] = int(Id)
        data['nivel_sim'] = data['nivel_sim'].round(3)
        data['caudal_sim'] = data['caudal_sim'].round(3)
        data.set_index(data['fecha'], inplace=True)
        del data['fecha']
        data.index = data.index.tz_localize(tz='America/Argentina/Buenos_Aires')
        data.index = data.index.tz_convert(tz="UTC")
        if Estac["CondBorde"] == "Stage Hydrograph":
            data = data.rename(columns={'nivel_sim':'valor'})
            del data["caudal_sim"]
        elif Estac["CondBorde"] == "Flow Hydrograph":
            data = data.rename(columns={'caudal_sim':'valor'})
            del data["nivel_sim"]
        df_SSalidas = pd.concat([df_SSalidas, data], ignore_index=False)
        if plotear == True:
            plt.plot(data['valor'],label=nombre)
            plt.xlabel('Fecha')
            plt.ylabel(Estac["CondBorde"].split(" ")[0])
            #plt.title(CBi['NomBBDD'])
            plt.legend()
            plt.show()
            plt.close()
    if pivot:
        return df_SSalidas.pivot(columns=["Id"])
    return df_SSalidas #,list_1


def print_h5_structure(f, level=0):
    '''    prints structure of hdf5 file    '''
    for key in f.keys():
        if isinstance(f[key], h5py._hl.dataset.Dataset):
            print(f"{'  '*level} DATASET: {f[key].name}")
        elif isinstance(f[key], h5py._hl.group.Group):
            print(f"{'  '*level} GROUP: {key, f[key].name}")
            level += 1
            print_h5_structure(f[key], level)
            level -= 1

        if f[key].parent.name == "/":
            print("\n"*2)

def FechasCorrida(rutaModelo,nombre_project,CurrentPlanFile):
    print ('\n Lee fechas de corrida')
    ruta_plan = rutaModelo+'/'+nombre_project+'.'+CurrentPlanFile
    f_plan = open(ruta_plan,'r')							#Abre el plan para leerlo
    for line in f_plan:										#Lee linea por linea el plan
        line = line.rstrip()
        if line.startswith('Simulation Date='):
            line = line.split('=')[1]
            line = line.split(',')
            
            f_inici0 = line[0]+' '+line[1]
            f_fin0 = line[2]+' '+line[3]
            break
    
    f_inici0 = pd.to_datetime(f_inici0)#, format='%Y-%m-%d')
    f_fin0 = pd.to_datetime(f_fin0)
    f_plan.close()
    return f_inici0,f_fin0



