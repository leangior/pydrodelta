import pydrodelta.a5 as a5
import pydrodelta.util as util
from datetime import timedelta 
import json
import numpy as np

class BoundarySerie():
    def __init__(self,params):
        self.series_id = params["series_id"]
        self.lim_outliers = params["lim_outliers"]
        self.lim_jump = params["lim_jump"]
        self.x_offset = util.interval2timedelta(params["x_offset"]) if isinstance(params["x_offset"],dict) else params["x_offset"] # shift_by
        self.y_offset = params["y_offset"]  # bias
    def loadData(self,timestart,timeend):
        self.data = a5.readSerie(self.series_id,timestart,timeend)
        if len(self.data["observaciones"]):
            self.obs_df = a5.observacionesListToDataFrame(self.data["observaciones"])
        else:
            print("Warning: no data found for series_id=%i" % self.series_id)
            self.obs_df = a5.createEmptyObsDataFrame()
    def removeOutliers(self):
        self.outliers_df = util.removeOutliers(self.obs_df,self.lim_outliers)
        if len(self.outliers_df):
            return True
        else:
            return False
    def detectJumps(self):
        self.jumps_df = util.detectJumps(self.obs_df,self.lim_jump)
        if len(self.jumps_df):
            return True
        else:
            return False
    def applyOffset(self):
        if isinstance(self.x_offset,timedelta):
            self.obs_df.index = [x + self.x_offset for x in self.obs_df.index]
        elif self.x_offset != 0:
            self.obs["valor"] = self.obs["valor"].shift(self.x_offset, axis = 0) 
        if self.y_offset != 0:
            self.obs_df["valor"] = self.obs_df["valor"] + self.y_offset
    def regularize(self,timestart,timeend,time_interval,time_offset):
        self.obs_df = util.serieRegular(self.obs_df,time_interval,timestart,timeend,time_offset)
    def fillNulls(self,other_obs_df,fill_value=None,x_offset=0,y_offset=0):
        self.obs_df = util.serieFillNulls(self.obs_df,other_obs_df,fill_value=fill_value,shift_by=x_offset,bias=y_offset)
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

class DerivedBoundarySerie:
    def __init__(self,params,boundary_set):
        self.series_id = params["series_id"] if params["series_id"] else None
        if "derived_from" in params:
            self.derived_from = DerivedOrigin(params["derived_from"],boundary_set)
        else:
            self.derived_from = None
        if "interpolated_from" in params:
            self.interpolated_from = InterpolatedOrigin(params["interpolated_from"],boundary_set)
        else:
            self.interpolated_from = None
    def derive(self,keep_index=True):
        if self.derived_from is not None:
            print("Deriving %i from %s" % (self.series_id, self.derived_from.origin.name))
            self.obs_df = self.derived_from.origin.series[0].obs_df[["valor",]]
            if isinstance(self.derived_from.x_offset,timedelta):
                self.obs_df["valor"] = self.obs_df["valor"] + self.derived_from.y_offset
                self.obs_df.index = [x + self.derived_from.x_offset for x in self.obs_df.index]
            else:
                self.obs_df["valor"] = self.obs_df["valor"].shift(self.derived_from.x_offset, axis = 0) + self.derived_from.y_offset
        elif self.interpolated_from is not None:
            print("Interpolating %i from %s and %s" % (self.series_id, self.interpolated_from.origin_1.name, self.interpolated_from.origin_2.name))
            self.obs_df = self.interpolated_from.origin_1.series[0].obs_df[["valor",]]
            self.obs_df = self.obs_df.join(self.interpolated_from.origin_2.series[0].obs_df[["valor",]],how='left',rsuffix="_other")
            self.obs_df["valor"] = self.obs_df["valor"] * (1 - self.interpolated_from.interpolation_coefficient) + self.obs_df["valor_other"] * self.interpolated_from.interpolation_coefficient
            del self.obs_df["valor_other"]
            if isinstance(self.interpolated_from.x_offset,timedelta):
                if keep_index:
                    self.obs_df = util.applyTimeOffsetToIndex(self.obs_df,self.interpolated_from.x_offset)
                else:
                    self.obs_df.index = [x + self.interpolated_from.x_offset for x in self.obs_df.index]
            else:
                self.obs_df["valor"] = self.obs_df["valor"].shift(self.interpolated_from.x_offset, axis = 0)    
            
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

class Boundary:
    def __init__(self,params,timestart=None,timeend=None):
        self.id = params["id"]
        self.name = params["name"]
        self.timestart = timestart
        self.timeend = timeend
        self.time_interval = util.interval2timedelta(params["time_interval"])
        self.time_offset = params["time_offset"] if "time_offset" in params else None
        self.fill_value = params["fill_value"] if "fill_value" in params else None
        self.output_series_id = params["output_series_id"] if "output_series_id" in params else None
        self.time_support = util.interval2timedelta(params["time_support"]) if "time_support" in params else None 
    def toCSV(self,include_series_id=False,include_header=True):
        if include_series_id:
            obs_df = self.series[0].obs_df
            obs_df["series_id"] = self.output_series_id
            return obs_df.to_csv(header=include_header)
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
        if self.output_series_id is not None:
            obs_created = a5.createObservaciones(obs_list,series_id=self.output_series_id)
            return obs_created
        else:
            print("Warning: missing output_series_id for boundary #%i, skipping upload" % self.id)
            return []
    def pivotData(self):
        df = self.series[0].obs_df[["valor",]]
        for serie in self.series:
            if len(serie.obs_df):
                df = df.join(serie.obs_df[["valor",]],how='outer',rsuffix="_%s" % serie.series_id,sort=True)
        del df["valor"]
        return df



class observedBoundary(Boundary):
    def __init__(self,params,timestart,timeend):
        super().__init__(params,timestart,timeend)
        self.series = [BoundarySerie(x) for x in params["series"]]
    def loadData(self,timestart,timeend):
        if self.series is not None:
            for serie in self.series:
                serie.loadData(timestart,timeend)
        elif self.derived_from is not None:
            self.series = []
            self.series[0] = util.se
    def removeOutliers(self):
        found_outliers = False
        for serie in self.series:
            found_outliers_ = serie.removeOutliers()
            found_outliers = found_outliers_ if found_outliers_ else found_outliers
        return found_outliers
    def detectJumps(self):
        found_jumps = False
        for serie in self.series:
            found_jumps_ = serie.detectJumps()
            found_jumps = found_jumps_ if found_jumps_ else found_jumps
        return found_jumps
    def applyOffset(self):
        for serie in self.series:
            serie.applyOffset()
    def regularize(self):
        for serie in self.series:
            serie.regularize(self.timestart,self.timeend,self.time_interval,self.time_offset)
    def fillNulls(self):
        if len(self.series) > 1:
            i = 2
            for serie in self.series[1:]:
                # if last, fills  
                fill_value = self.fill_value if i == len(self.series) else None 
                self.series[0].fillNulls(serie.obs_df,fill_value) # ,serie.x_offset,serie.y_offset)
                i = i + 1
        else:
            print("Warning: no other series to fill nulls with")

class derivedBoundary(Boundary):
    def __init__(self,params,timestart,timeend,parent):
        super().__init__(params,timestart,timeend)
        if "derived_from" in params:
            self.series = [DerivedBoundarySerie({"series_id":self.output_series_id, "derived_from": params["derived_from"]},parent)]
        elif "interpolated_from" in params:
            self.series = [DerivedBoundarySerie({"series_id":self.output_series_id, "interpolated_from": params["interpolated_from"]},parent)]
    def derive(self):
        self.series[0].derive()

class DerivedOrigin:
    def __init__(self,params,boundary_set=None):
        self.boundary_id = params["boundary_id"]
        self.x_offset = params["x_offset"]
        self.y_offset = params["y_offset"]
        if boundary_set is not None:
            from_boundaries = [x for x in boundary_set.boundaries if x.id == self.boundary_id]
            if not len(from_boundaries):
                raise Exception("origin boundary not found for derived boundary, id: %i" % self.boundary_id)
            self.origin = from_boundaries[0]
        else:
            self.origin = None

class InterpolatedOrigin:
    def __init__(self,params,boundary_set=None):
        self.boundary_id_1 = params["boundary_id_1"]
        self.boundary_id_2 = params["boundary_id_2"]
        self.x_offset = params["x_offset"]
        self.y_offset = params["y_offset"]
        self.interpolation_coefficient = params["interpolation_coefficient"]
        if boundary_set is not None:
            from_boundaries = [x for x in boundary_set.boundaries if x.id == self.boundary_id_1]
            if not len(from_boundaries):
                raise Exception("origin boundary not found for interpolated boundary, id: %i" % self.interpolated_from.boundary_id_1)
            self.origin_1 = from_boundaries[0]
            from_boundaries = [x for x in boundary_set.boundaries if x.id == self.boundary_id_2]
            if not len(from_boundaries):
                raise Exception("origin boundary not found for interpolated boundary, id: %i" % self.boundary_id_2)
            self.origin_2 = from_boundaries[0]
        else:
            self.origin_1 = None
            self.origin_2 = None

# class BordeSetIterator:
#     def __init__(self,borde_set):
#         self._borde_set = borde_set
#         self.index = 0
#     def __next__(self):
#         if self.index < len(self._borde_set.bordes):
#             return self._borde_set.bordes[self.index]
#         raise StopIteration


class BoundarySet():
    def __init__(self,params):
        self.timestart = util.tryParseAndLocalizeDate(params["timestart"])
        self.timeend = util.tryParseAndLocalizeDate(params["timeend"])
        self.boundaries = []
        for x in params["boundaries"]:
            self.boundaries.append(derivedBoundary(x,self.timestart,self.timeend,self) if "derived" in x and x["derived"] == True else observedBoundary(x,self.timestart,self.timeend))
    def addBoundary(self,boundary):
        self.boundaries.append(derivedBoundary(boundary,self.timestart,self.timeend,self) if "derived" in boundary and boundary["derived"] == True else observedBoundary(boundary,self.timestart,self.timeend))
    def batchProcessInput(self):
        self.loadData()
        self.removeOutliers()
        self.detectJumps()
        self.applyOffset()
        self.regularize()
        self.fillNulls()
        self.derive()
    def loadData(self):
        for boundary in self.boundaries:
            if isinstance(boundary,observedBoundary):
                boundary.loadData(self.timestart,self.timeend)
    def removeOutliers(self):
        found_outliers = False
        for boundary in self.boundaries:
            if isinstance(boundary,observedBoundary):
                found_outliers_ = boundary.removeOutliers()
                found_outliers = found_outliers_ if found_outliers_ else found_outliers
        return found_outliers
    def detectJumps(self):
        found_jumps = False
        for boundary in self.boundaries:
            if isinstance(boundary,observedBoundary):
                found_jumps_ = boundary.detectJumps()
                found_jumps = found_jumps_ if found_jumps_ else found_jumps
        return found_jumps
    def applyOffset(self):
        for boundary in self.boundaries:
            if isinstance(boundary,observedBoundary):
                boundary.applyOffset()
    def regularize(self):
        for boundary in self.boundaries:
            if isinstance(boundary,observedBoundary):
                boundary.regularize()
    def fillNulls(self):
        for boundary in self.boundaries:
            if isinstance(boundary,observedBoundary):
                boundary.fillNulls()
    def derive(self):
        for boundary in self.boundaries:
            if isinstance(boundary,derivedBoundary):
                boundary.derive()
    def toCSV(self,pivot=False):
        if pivot:
            df = self.pivotData()
            df["timestart"] = [x.isoformat() for x in df.index]
            df.reset_index
            return df.to_csv(index=False)    
        header = ",".join(["timestart","valor","series_id"])
        return header + "\n" + "\n".join([boundary.toCSV(True,False) for boundary in self.boundaries])
    def toList(self,pivot=False):
        if pivot:
            df = self.pivotData()
            df["timestart"] = [x.isoformat() for x in df.index]
            df.reset_index
            df["timeend"] = df["timestart"]
            return df.to_dict(orient="records")
        obs_list = []
        for boundary in self.boundaries:
            obs_list.extend(boundary.toList(True))
        return obs_list
    def saveData(self,file : str,format="csv",pivot=False):
        f = open(file,"w")
        if format == "json":
            obs_json = json.dumps(self.toList(pivot),ensure_ascii=False)
            f.write(obs_json)
            f.close()
            return
        f.write(self.toCSV(pivot))
        f.close
        return
    def uploadData(self):
        created = []
        for boundary in self.boundaries:
            obs_created = boundary.uploadData()
            created.extend(obs_created)
        return created
    def pivotData(self):
        df = self.boundaries[0].series[0].obs_df[["valor",]]
        for boundary in self.boundaries:
            if len(boundary.series) and len(boundary.series[0].obs_df):
                df = df.join(boundary.series[0].obs_df[["valor",]][boundary.series[0].obs_df.valor.notnull()],how='outer',rsuffix="_%s" % boundary.name,sort=True)
        del df["valor"]
        df = df.replace({np.NaN:None})
        return df
    # def __iter__(self):
    #     return BordeSetIterator(self)

