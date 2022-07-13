## pydrodelta

módulo generación de análisis de series temporales

- se definen las clases NodeSerie, Node y Topology
- se define un esquema json para la configuración de la topología (schemas/topology.json)
- se instancia una clase Topology usando el archivo json de configuración y:
- lee series de entrada de a5 topology.loadData()
- regulariza las series .regularize()
- rellena nulos .fillNulls()
- guarda observaciones en archivo csv o json .saveData()
- guarda observaciones en a5 .uploadData() 

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
    t_config = json.load(open("pydrodelta_config/288_bordes_curados.json"))
    topology = pydrodelta.analysis.Topology(t_config])
    topology.loadData()
    topology.regularize()
    topology.fillNulls()
    # csv = topology.toCSV()
    topology.saveData("bordes_288.csv",pivot=True)
    topology.saveData("bordes_288.json","json")
    topology.uploadData()

#### CLI

    python3 cli.py pydrodelta_config/288_bordes_curados.json -u -p -c bordes_288.csv

#### References

Instituto Nacional del Agua

Subgerencia de Sistemas de Información y Alerta Hidrológico

Argentina

2022