import pydrodelta.analysis 
import json

## GENERA BORDE HIDRODELTA

def borde_hidrodelta():
    bordes_config = json.load(open("pydrodelta_config/288_config.json"))
    bordes_set = pydrodelta.analysis.BoundarySet(bordes_config["boundaries"])
    bordes_set.loadData()
    outliers = bordes_set.boundaries[0].series[0].removeOutliers()
    bordes_set.regularize()
    bordes_set.fillNulls()
    # csv = bordes_set.toCSV()
    bordes_set.saveData("bordes_288.csv")
    bordes_set.saveData("bordes_288.json","json")
    #upload all
    bordes_set.uploadData()
    # upload only one boundary
    bordes_set.boundaries[2].uploadData()

## GENERA BORDE SAN FERNANDO

def borde_sfer():
    borde_config = {
        "series":[
            {"series_id": 85, "lim_outliers":[-3.03,5.0],"lim_jump":0.835,"x_offset":0,"y_offset":0},
            {"series_id": 52, "lim_outliers":[-2.25,4.62],"lim_jump":0.958,"x_offset":0,"y_offset":0},
            {"series_id": 3279, "lim_outliers":[-1.82,4.18],"lim_jump":0.8,"x_offset":0,"y_offset":0},
            {"series_id": 3280, "lim_outliers":[-1.7,3.42],"lim_jump":0.4,"x_offset":0,"y_offset":0},
            {"series_id": 2111, "lim_outliers":[-2.01,2.66],"lim_jump":0.5,"x_offset":0,"y_offset":0}
            ],
        "time_interval": timedelta(hours=1),
        "timestart": "2022-05-01",
        "timeend": "2022-06-01",
        "time_offset": None}
    borde = pydrodelta.analysis.Boundary(borde_config)
    borde.loadData()
    borde.regularize()
    borde.fillNulls()
    f = open("borde_sfer.csv","w")
    f.write(borde.series[0].obs_df.to_csv())
    f.close()
    return borde

## GENERA BORDE LUJAN (Q 1D)

def borde_luj():
    borde_config = {
        "series":[
            {"series_id": 26711, "lim_outliers":[0.0,1000.0],"lim_jump":1000.0,"x_offset":0,"y_offset":0}, # LUJAN @ JAUREGUI QmedDiaria UNLU (actual)
            {"series_id": 31536, "lim_outliers":[0.0,10000.0],"lim_jump":1000.0,"x_offset":0,"y_offset":0} # LUJAN @ JAUREGUI QmedDiaria DPH (viejo) 
            ],
        "time_interval": timedelta(days=1),
        "timestart": "1990-01-01",
        "timeend": "2022-06-01",
        "time_offset": None,
        "fill_value": 50}
    borde = pydrodelta.analysis.Boundary(borde_config)
    borde.loadData()
    borde.regularize()
    borde.fillNulls()
    f = open("borde_luj.csv","w")
    f.write(borde.series[0].obs_df.to_csv())
    f.close()
    return borde