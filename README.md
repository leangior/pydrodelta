## pydrodelta

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

#### boundary condition generation

    import pydrodelta.analysis
    bordes_config = json.load(open("pydrodelta_config/288_config.json"))
    bordes_set = pydrodelta.analysis.BoundarySet(bordes_config["boundaries"])
    bordes_set.loadData()
    bordes_set.regularize()
    bordes_set.fillNulls()
    # csv = bordes_set.toCSV()
    bordes_set.saveData("bordes_288.csv")
    bordes_set.saveData("bordes_288.json","json")
    bordes_set.uploadData()

