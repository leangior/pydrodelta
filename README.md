## pydrodelta

módulo generación de análisis de series temporales

- se definen las clases NodeSerie, Node y Topology
- se define un esquema json (jsonschema) para validar la configuración de la topología (data/schemas/topology.json)
- se instancia un objeto de la clase Topology usando el archivo json de configuración y:
- lee series de entrada de a5 topology.loadData()
- regulariza las series topology.regularize()
- rellena nulos topology.fillNulls()
- guarda observaciones en archivo csv o json topology.saveData()
- guarda observaciones en a5 topology.uploadData() 

módulo simulación

- se definen las clases Plan, Procedure y clases para cada procedimiento específico
- se define un esquema json (jsonschema) para validar la configuración del plan (data/schemas/plan.json) 
- se instancia un objeto de la clase Plan usando el archivo de configuración, y:
- ejecuta el análisis de la topología del plan (generación de condiciones de borde, plan.topology.batchProcessInput())
- ejecuta secuencialmente los procedimientos (procedure.run() por cada procedure en plan.procedures)

### Description

La aplicación lee un archivo de entrada .json que define con qué armar las series, de acuerdo al esquema definido acá: https://github.com/jbianchi81/pydrodelta/blob/main/schemas/topology.json . Por ejemplo, para el modelo Hidrodelta el archivo de entrada es así: https://github.com/jbianchi81/pydrodelta/blob/main/pydrodelta_config/288_bordes_curados.json . Básicamente, el esquema define una topología que contiene 1..n nodos, cada uno de los cuales 1..n series. También se pueden definir nodos derivados allí donde no hay observaciones, copiando o interpolando otros nodos. Luego de descargar de la base de datos, curar y regularizar las series, los datos faltantes de la primera serie se completan con los de las subsiguientes, para dar una serie resultante por nodo. Luego se calculan los nodos derivados y finalmente se exportan las series a .csv, .json y/o se cargan a la base de datos.

### installation

download pydrodelta-0.0.1.tar.gz
extract content into $PROJECT_DIR

    cd $PROJECT_DIR
    python3 -m venv myenv
    source myenv/bin/activate
    python3 -m pip install .
    export PYDRODELTA_DIR=$PWD
    cp config/config_empty.json config/config.json
    nano config/config.json # <- input api connection parameters

### use examples

#### a5 api series to/from dataframe

    import pydrodelta.a5 as a5
    # lee serie de api a5
    serie = a5.readSerie(31532,"2022-05-25T03:00:00Z","2022-06-01T03:00:00Z")
    # convierte observaciones a dataframe 
    obs_df = a5.observacionesListToDataFrame(serie["observaciones"]) 
    # convierte de dataframe a lista de dict
    obs_list = a5.observacionesDataFrameToList(obs_df,series_id=serie["id"])
    # valida observaciones
    for x in obs_list:
        a5.validate(x,"Observacion")
    # sube observaciones a la api a5
    upserted = a5.createObservaciones(obs_df,series_id=serie["id"])

#### python api analysis de series temporales

    import pydrodelta.analysis
    import json

    t_config = json.load(open("pydrodelta_config/288_bordes_curados.json"))
    topology = pydrodelta.analysis.Topology(t_config])
    topology.loadData()
    topology.regularize()
    topology.fillNulls()
    # csv = topology.toCSV()
    topology.saveData("bordes_288.csv",pivot=True)
    topology.saveData("bordes_288.json","json")
    topology.uploadData()

#### python api simulation

    import pydrodelta.simulation
    import json

    plan_config = json.load(open("../data/plans/gualeguay_rt_dummy.json"))
    plan = pydrodelta.simulation.Plan(plan_config)
    plan.execute()

#### CLI

    pydrodelta run_analysis pydrodelta_config/288_bordes_curados.json -u -p -c bordes_288.csv

#### References

Instituto Nacional del Agua

Subgerencia de Sistemas de Información y Alerta Hidrológico

Argentina

2022