from xml.etree.ElementInclude import include
import pydrodelta.a5 as a5
import pydrodelta.util as util
from datetime import timedelta 
import json

class BoundarySerie():
    def __init__(self,params):
        self.series_id = params["series_id"]
        self.lim_outliers = params["lim_outliers"]
        self.lim_jump = params["lim_jump"]
        self.x_offset = params["x_offset"]  # shift_by
        self.y_offset = params["y_offset"]  # bias
    def loadData(self,timestart,timeend):
        self.data = a5.readSerie(self.series_id,timestart,timeend)
        if len(self.data["observaciones"]):
            self.obs_df = a5.observacionesListToDataFrame(self.data["observaciones"])
        else:
            print("Warning: no data found for series_id=%i" % self.series_id)
    def regularize(self,timestart,timeend,time_interval,time_offset):
        self.obs_df = util.serieRegular(self.obs_df,time_interval,timestart,timeend,time_offset)
    def fillNulls(self,other_obs_df,fill_value=None,x_offset=None,y_offset=None):
        self.obs_df = util.serieFillNulls(self.obs_df,other_obs_df,fill_value=fill_value,shift_by=x_offset,bias=y_offset)
    def removeOutliers(self):
        # TODO
        pass
    def detectJumps(self):
        # TODO
        pass
    def toCSV(self,include_series_id=False):
        if include_series_id:
            obs_df = self.obs_df
            obs_df["series_id"] = self.series_id
            return obs_df.to_csv()
        return self.obs_df.to_csv()
    def toList(self,include_series_id=False,timeSupport=None):
        obs_df = self.obs_df
        obs_df["timestart"] = obs_df.index
        obs_df["timeend"] = [x + timeSupport for x in obs_df["timestart"]] if timeSupport is not None else obs_df["timestart"]
        obs_df["timestart"] = [x.isoformat() for x in obs_df["timestart"]]
        obs_df["timeend"] = [x.isoformat() for x in obs_df["timeend"]]
        if include_series_id:
            obs_df["series_id"] = self.series_id
        return obs_df.to_dict(orient="records")

class Boundary():
    def __init__(self,params):
        self.series = [BoundarySerie(x) for x in params["series"]]
        self.time_interval = util.interval2timedelta(params["time_interval"])
        self.timestart = util.tryParseAndLocalizeDate(params["timestart"])
        self.timeend = util.tryParseAndLocalizeDate(params["timeend"])
        self.time_offset = params["time_offset"] if "time_offset" in params else None
        self.fill_value = params["fill_value"] if "fill_value" in params else None
        self.output_series_id = params["output_series_id"] if "output_series_id" in params else None
        self.time_support = util.interval2timedelta(params["time_support"]) if "time_support" in params else None 
    def loadData(self):
        for serie in self.series:
            serie.loadData(self.timestart,self.timeend)
    def regularize(self):
        for serie in self.series:
            serie.regularize(self.timestart,self.timeend,self.time_interval,self.time_offset)
    def fillNulls(self):
        if len(self.series) > 1:
            i = 2
            for serie in self.series[1:]:
                # if last, fills  
                fill_value = self.fill_value if i == len(self.series) else None 
                self.series[0].fillNulls(serie.obs_df,fill_value,serie.x_offset,serie.y_offset)
                i = i + 1
        else:
            print("Warning: no other series to fill nulls with")
    def toCSV(self,include_series_id=False):
        if include_series_id:
            obs_df = self.series[0].obs_df
            obs_df["series_id"] = self.output_series_id
            return obs_df.to_csv()
        return self.series[0].toCSV()
    def toList(self,include_series_id=False):
        obs_df = self.series[0].obs_df[self.series[0].obs_df.valor.notnull()]
        obs_df["timestart"] = obs_df.index
        obs_df["timeend"] = [x + self.time_support for x in obs_df["timestart"]] if self.time_support is not None else obs_df["timestart"]
        obs_df["timestart"] = [x.isoformat() for x in obs_df["timestart"]]
        obs_df["timeend"] = [x.isoformat() for x in obs_df["timeend"]]
        if include_series_id:
            obs_df["series_id"] = self.output_series_id
        return obs_df.to_dict(orient="records")
    def uploadData(self):
        obs_list = self.toList()
        obs_created = a5.createObservaciones(obs_list,series_id=self.output_series_id)
        return obs_created

# class BordeSetIterator:
#     def __init__(self,borde_set):
#         self._borde_set = borde_set
#         self.index = 0
#     def __next__(self):
#         if self.index < len(self._borde_set.bordes):
#             return self._borde_set.bordes[self.index]
#         raise StopIteration


class BoundarySet():
    def __init__(self,boundaries):
        self.boundaries = [Boundary(x) for x in boundaries]
    def addBoundary(self,boundary):
        self.boundaries.append(Boundary(boundary))
    def loadData(self):
        for boundary in self.boundaries:
            boundary.loadData()
    def regularize(self):
        for boundary in self.boundaries:
            boundary.regularize()
    def fillNulls(self):
        for boundary in self.boundaries:
            boundary.fillNulls()
    def toCSV(self):
        return "\n".join([boundary.toCSV(True) for boundary in self.boundaries])
    def toList(self):
        obs_list = []
        for boundary in self.boundaries:
            obs_list.extend(boundary.toList(True))
        return obs_list
    def saveData(self,file : str,format="csv"):
        f = open(file,"w")
        if format == "json":
            obs_json = json.dumps(self.toList())
            f.write(obs_json)
            f.close()
            return
        f.write(self.toCSV())
        f.close
        return
    def uploadData(self):
        created = []
        for boundary in self.boundaries:
            obs_created = boundary.uploadData()
            created.extend(obs_created)
        return created
    # def __iter__(self):
    #     return BordeSetIterator(self)

