import pydrodelta.a5 as a5
import pydrodelta.util as util
from datetime import timedelta, datetime 
import json
import numpy as np
import matplotlib.pyplot as plt
import logging
import pandas
import jsonschema
import os

schema = open("%s/data/schemas/topology.json" % os.environ["PYDRODELTA_DIR"])
schema = json.load(schema)

config_file = open("%s/config/config.json" % os.environ["PYDRODELTA_DIR"]) # "src/pydrodelta/config/config.json")
config = json.load(config_file)
config_file.close()

logging.basicConfig(filename="%s/%s" % (os.environ["PYDRODELTA_DIR"],config["log"]["filename"]), level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
logging.FileHandler("%s/%s" % (os.environ["PYDRODELTA_DIR"],config["log"]["filename"]),"w+")

class NodeSerie():
    def __init__(self,params):
        self.series_id = params["series_id"]
        self.type = params["tipo"] if "tipo" in params else "puntual"
        self.lim_outliers = params["lim_outliers"] if "lim_outliers" in params else None
        self.lim_jump = params["lim_jump"] if "lim_jump" in params else None
        self.x_offset = timedelta(seconds=0) if "x_offset" not in params else util.interval2timedelta(params["x_offset"]) if isinstance(params["x_offset"],dict) else params["x_offset"] # shift_by
        self.y_offset = params["y_offset"]  if "y_offset" in params else 0 # bias
        self.moving_average = util.interval2timedelta(params["moving_average"]) if "moving_average" in params else None
        self.data = None
    def loadData(self,timestart,timeend):
        logging.debug("Load data for series_id: %i" % (self.series_id))
        self.data = a5.readSerie(self.series_id,timestart,timeend,tipo=self.type)
        if len(self.data["observaciones"]):
            self.data = a5.observacionesListToDataFrame(self.data["observaciones"],tag="obs")
        else:
            logging.warning("No data found for series_id=%i" % self.series_id)
            self.data = a5.createEmptyObsDataFrame(extra_columns={"tag":"str"})
        self.original_data = self.data.copy(deep=True)
    def removeOutliers(self):
        if self.lim_outliers is None:
            return False
        self.outliers_data = util.removeOutliers(self.data,self.lim_outliers)
        if len(self.outliers_data):
            return True
        else:
            return False
    def detectJumps(self):
        if self.lim_jump is None:
            return False
        self.jumps_data = util.detectJumps(self.data,self.lim_jump)
        if len(self.jumps_data):
            return True
        else:
            return False
    def applyMovingAverage(self):
        if self.moving_average is not None:
            # self.data["valor"] = util.serieMovingAverage(self.data,self.moving_average)
            self.data = util.serieMovingAverage(self.data,self.moving_average,tag_column = "tag")
    def applyTimedeltaOffset(self,row,x_offset):
        return row.name + x_offset
    def applyOffset(self):
        if isinstance(self.x_offset,timedelta):
            self.data.index = self.data.apply(lambda row: self.applyTimedeltaOffset(row,self.x_offset), axis=1) # for x in self.data.index]
            self.data.index.rename("timestart",inplace=True)
        elif self.x_offset != 0:
            self.data["valor"] = self.data["valor"].shift(self.x_offset, axis = 0) 
            self.data["tag"] = self.data["tag"].shift(self.x_offset, axis = 0) 
        if self.y_offset != 0:
            self.data["valor"] = self.data["valor"] + self.y_offset
    def regularize(self,timestart,timeend,time_interval,time_offset,interpolation_limit,inline=True,interpolate=False):
        data = util.serieRegular(self.data,time_interval,timestart,timeend,time_offset,interpolation_limit=interpolation_limit,tag_column="tag",interpolate=interpolate)
        if inline:
            self.data = data
        else:
            return data
    def fillNulls(self,other_data,fill_value=None,x_offset=0,y_offset=0,inline=False):
        data = util.serieFillNulls(self.data,other_data,fill_value=fill_value,shift_by=x_offset,bias=y_offset,tag_column="tag")
        if inline:
            self.data = data
        else:
            return data
    def toCSV(self,include_series_id=False):
        if include_series_id:
            data = self.data
            data["series_id"] = self.series_id
            return data.to_csv()
        return self.data.to_csv()
    def toList(self,include_series_id=False,timeSupport=None,remove_nulls=False,max_obs_date:datetime=None):
        data = self.data[self.data.index <= max_obs_date] if max_obs_date is not None else self.data.copy(deep=True)
        data["timestart"] = data.index
        data["timeend"] = [x + timeSupport for x in data["timestart"]] if timeSupport is not None else data["timestart"]
        data["timestart"] = [x.isoformat() for x in data["timestart"]]
        data["timeend"] = [x.isoformat() for x in data["timeend"]]
        if include_series_id:
            data["series_id"] = self.series_id
        obs_list = data.to_dict(orient="records")
        for obs in obs_list:
            obs["valor"] = None if pandas.isna(obs["valor"]) else obs["valor"]
            obs["tag"] = None if pandas.isna(obs["valor"]) else obs["valor"]
        if remove_nulls:
            obs_list = [x for x in obs_list if x["valor"] is not None] # remove nulls
        return obs_list

class NodeSerieProno(NodeSerie):
    def __init__(self,params):
        super().__init__(params)        
        self.cal_id = params["cal_id"]
        self.qualifier = params["qualifier"] if "qualifier" in params else None
        self.adjust = params["adjust"] if "adjust" in params else False
        self.cor_id = None
        self.forecast_date = None
        self.adjust_results = None
        self.name = "cal_id: %i, %s" % (self.cal_id, self.qualifier)
    def loadData(self,timestart,timeend):
        logging.debug("Load prono data for series_id: %i, cal_id: %i" % (self.series_id, self.cal_id))
        self.data = a5.readSerieProno(self.series_id,self.cal_id,timestart,timeend,qualifier=self.qualifier)
        if len(self.data["pronosticos"]):
            self.data = a5.observacionesListToDataFrame(self.data["pronosticos"],tag="prono")
        else:
            logging.warning("No data found for series_id=%i, cal_id=%i" % (self.series_id, self.cal_id))
            self.data = a5.createEmptyObsDataFrame()
        self.original_data = self.data.copy(deep=True)
    def setData(self,data):
        self.data = data

class DerivedNodeSerie:
    def __init__(self,params,topology):
        self.series_id = params["series_id"] if params["series_id"] else None
        if "derived_from" in params:
            self.derived_from = DerivedOrigin(params["derived_from"],topology)
        else:
            self.derived_from = None
        if "interpolated_from" in params:
            self.interpolated_from = InterpolatedOrigin(params["interpolated_from"],topology)
        else:
            self.interpolated_from = None
    def deriveTag(self,row,tag_column,tag="derived"):
        if row[tag_column] is None:
            return tag
        else:
            return "%s,%s" % (row[tag_column], tag)
    def deriveOffsetIndex(self,row,x_offset):
        return row.name + x_offset
    def derive(self,keep_index=True):
        if self.derived_from is not None:
            logging.debug("Deriving %i from %s" % (self.series_id, self.derived_from.origin.name))
            if not len(self.derived_from.origin.data):
                logging.warn("No data found to derive from origin. Skipping derived node")
                self.data = a5.createEmptyObsDataFrame()
                return
            self.data = self.derived_from.origin.data[["valor","tag"]] # self.derived_from.origin.series[0].data[["valor",]]
            if isinstance(self.derived_from.x_offset,timedelta):
                self.data["valor"] = self.data["valor"] + self.derived_from.y_offset
                self.data.index = self.data.apply(lambda row: self.deriveOffsetIndex(row,self.derived_from.x_offset),axis=1)# for x in self.data.index]
                self.data["tag"] = self.data.apply(lambda row: self.deriveTag(row,"tag"),axis=1) # ["derived" if x is None else "%s,derived" % x for x in self.data.tag]
            else:
                self.data["valor"] = self.data["valor"].shift(self.derived_from.x_offset, axis = 0) + self.derived_from.y_offset
            self.data["tag"] = self.data.apply(lambda row: self.deriveTag(row,"tag"),axis=1) #["derived" if x is None else "%s,derived" % x for x in self.data.tag]
            if hasattr(self.derived_from.origin,"max_obs_date"):
                self.max_obs_date = self.derived_from.origin.max_obs_date
        elif self.interpolated_from is not None:
            logging.debug("Interpolating %i from %s and %s" % (self.series_id, self.interpolated_from.origin_1.name, self.interpolated_from.origin_2.name))
            if not len(self.interpolated_from.origin_1.data) or not len(self.interpolated_from.origin_2.data):
                logging.warn("No data found to derive from origin. Skipping derived node")
                self.data = a5.createEmptyObsDataFrame()
                return
            self.data = self.interpolated_from.origin_1.data[["valor","tag"]] # self.interpolated_from.origin_1.series[0].data[["valor",]]
            self.data = self.data.join(self.interpolated_from.origin_2.data[["valor","tag"]],how='left',rsuffix="_other") # self.data.join(self.interpolated_from.origin_2.series[0].data[["valor",]],how='left',rsuffix="_other")
            self.data["valor"] = self.data["valor"] * (1 - self.interpolated_from.interpolation_coefficient) + self.data["valor_other"] * self.interpolated_from.interpolation_coefficient
            self.data["tag"] = self.data.apply(lambda row: self.deriveTag(row,"tag","interpolated"),axis=1) #["interpolated" if x is None else "%s,interpolated" % x for x in self.data.tag]
            del self.data["valor_other"]
            del self.data["tag_other"]
            if isinstance(self.interpolated_from.x_offset,timedelta):
                if keep_index:
                    self.data = util.applyTimeOffsetToIndex(self.data,self.interpolated_from.x_offset)
                else:
                    self.data.index = self.data.apply(lambda row: self.deriveOffsetIndex(row,self.interpolated_from.x_offset),axis=1) # for x in self.data.index]
            else:
                self.data[["valor","tag"]] = self.data[["valor","tag"]].shift(self.interpolated_from.x_offset, axis = 0)    
            if hasattr(self.interpolated_from.origin_1,"max_obs_date"):
                self.max_obs_date = self.interpolated_from.origin_1.max_obs_date
            
    def toCSV(self,include_series_id=False):
        if include_series_id:
            data = self.data
            data["series_id"] = self.series_id
            return data.to_csv()
        return self.data.to_csv()
    def toList(self,include_series_id=False,timeSupport=None):
        data = self.data
        data["timestart"] = data.index
        data["timeend"] = [x + timeSupport for x in data["timestart"]] if timeSupport is not None else data["timestart"]
        data["timestart"] = [x.isoformat() for x in data["timestart"]]
        data["timeend"] = [x.isoformat() for x in data["timeend"]]
        if include_series_id:
            data["series_id"] = self.series_id
        return data.to_dict(orient="records")

class Node:
    def __init__(self,params,timestart=None,timeend=None,forecast_timeend=None,plan=None,time_offset=None):
        if "id" not in params:
            raise ValueError("id of node must be defined")
        self.id = params["id"]
        self.tipo = params["tipo"] if "tipo" in params else "puntual"
        if "name" not in params:
            raise ValueError("name of node must be defined")
        self.name = params["name"]
        self.timestart = timestart
        self.timeend = timeend
        self.forecast_timeend = forecast_timeend
        if "time_interval" not in params:
            raise ValueError("time_interval of node must be defined")
        self.time_interval = util.interval2timedelta(params["time_interval"])
        self.time_offset = time_offset if time_offset is not None else util.interval2timedelta(params["time_offset"]) if "time_offset" in params and params["time_offset"] is not None else None
        self.fill_value = params["fill_value"] if "fill_value" in params else None
        self.series_output = [NodeSerie(x) for x in params["series_output"]] if "series_output" in params else [NodeSerie({"series_id": params["output_series_id"]})] if "output_series_id" in params else None
        self.series_sim = None
        if "series_sim" in params:
            self.series_sim = []
            for serie in params["series_sim"]:
                serie["cal_id"] = serie["cal_id"] if "cal_id" in serie else plan.id if plan is not None else None
                self.series_sim.append(NodeSerieProno(serie))
        self.time_support = util.interval2timedelta(params["time_support"]) if "time_support" in params else None 
        self.adjust_from = params["adjust_from"] if "adjust_from" in params else None
        self.linear_combination = params["linear_combination"] if "linear_combination" in params else None
        self.interpolation_limit = params["interpolation_limit"] if "interpolation_limit" in params else None # in rows
        if self.interpolation_limit is not None and self.interpolation_limit <= 0:
            raise("Invalid interpolation_limit: must be greater than 0")
        self.data = None
        self.original_data = None
        self.adjust_results = None
        self.hec_node = params["hec_node"] if "hec_node" in params else None
    def createDatetimeIndex(self):
        return util.createDatetimeSequence(None, self.time_interval, self.timestart, self.timeend, self.time_offset)
    def toCSV(self,include_series_id=False,include_header=True):
        """
        returns self.data as csv
        """
        data = self.data[["valor","tag"]] # self.concatenateProno(inline=False) if include_prono else self.data[["valor","tag"]] # self.series[0].data            
        if include_series_id:
            data["series_id"] = self.series_output[0].series_id
        return data.to_csv(header=include_header) # self.series[0].toCSV()
    def outputToCSV(self,include_header=True):
        """
        returns data of self.series_output as csv
        """
        data = self.mergeOutputData()
        return data.to_csv(header=include_header) # self.series[0].toCSV()
    def toSerie(self,include_series_id=False,use_node_id=False):
        """
        return node as Serie object using self.data as observaciones
        """
        observaciones = self.toList(include_series_id=include_series_id,use_node_id=use_node_id)
        series_id = self.series_output[0].series_id if not use_node_id else self.id
        return a5.Serie({
            "tipo": self.tipo,
            "id": series_id,
            "observaciones": observaciones
        })
    def toList(self,include_series_id=False,use_node_id=False): #,include_prono=False):
        """
        returns self.data as list of dict
        """
        # data = self.concatenateProno(inline=False) if include_prono else self.data[["valor","tag"]] # self.series[0].data[self.series[0].data.valor.notnull()]
        data = self.data[self.data.valor.notnull()].copy()
        data.loc[:,"timestart"] = data.index
        data.loc[:,"timeend"] = [x + self.time_support for x in data["timestart"]] if self.time_support is not None else data["timestart"]
        data.loc[:,"timestart"] = [x.isoformat() for x in data["timestart"]]
        data.loc[:,"timeend"] = [x.isoformat() for x in data["timeend"]]
        if include_series_id:
            # if self.series_output is not None:
            #     obs = []
            #     for serie in self.series_output:
            #         data_ = data[["valor","timestart","timend"]]
            #         data_.loc[:,"series_id"] = serie.series_id
            #         obs.extend(data_.to_dict(orient="records"))
            # elif self.output_series_id is not None:
            data.loc[:,"series_id"] = self.series_output[0].series_id if not use_node_id else self.id
            # else:
            #     logging.warn("Missing output_series_id for node %s" % str(self.id))
        return data.to_dict(orient="records")
    def mergeOutputData(self):
        """
        merges data of all self.series_output into a single dataframe
        """
        data = None
        i = 0
        for serie in self.series_output:
            i = i + 1
            series_data = serie.data[["valor","tag"]]
            series_data["series_id"] = serie.series_id
            data = series_data if i == 1 else pandas.concat([data,series_data],axis=0)
        return data
    def outputToList(self,flatten=True):
        """
        returns series_output as list of dict
        if flatten == True, merges observations into single list. Else, returns list of series objects: [{series_id:int, observaciones:[{obs1},{obs2},...]},...]
        """
        if self.series_output[0].data is None:
            self.setOutputData()
        list = []
        for serie in self.series_output:
            data = serie.data[serie.data.valor.notnull()].copy()
            #data.loc[:,"timestart"] = data.index.copy()
            data.reset_index(inplace=True)
            data["timeend"] = data["timestart"] + self.time_support if self.time_support is not None else data["timestart"].copy() # [x + self.time_support for x in data.loc[:,"timestart"]] if self.time_support is not None else data.loc[:,"timestart"].copy()
            data.loc[:,"timestart"] = [x.isoformat() for x in data.loc[:,"timestart"]]
            data.loc[:,"timeend"] = [x.isoformat() for x in data.loc[:,"timeend"]]
            if flatten:
                data.loc[:,"series_id"] = serie.series_id
                list.extend(data.to_dict(orient="records"))
            else:
                series_table = "series" if serie.type == "puntual" else "series_areal" if serie.type == "areal" else "series_rast" if serie.type == "raster" else "series"
                list.append({"series_id": serie.series_id, "series_table": series_table, "observaciones": data.to_dict(orient="records")})
        return list
    def adjust(self,plot=True):
        truth_data = self.series[self.adjust_from["truth"]].data
        sim_data = self.series[self.adjust_from["sim"]].data
        self.series[self.adjust_from["sim"]].original_data = sim_data.copy(deep=True)
        try:
            adj_serie, tags, model = util.adjustSeries(sim_data,truth_data,method=self.adjust_from["method"],plot=plot,tag_column="tag",title=self.name)
        except ValueError:
            logging.warning("No observations found to estimate coefficients. Skipping adjust")
            return
        # self.series[self.adjust_from["sim"]].data["valor"] = adj_serie
        self.data.loc[:,"valor"] = adj_serie
        self.data.loc[:,"tag"] = tags
        self.adjust_results = model
    def apply_linear_combination(self,plot=True,series_index=0):
        self.series[series_index].original_data = self.series[series_index].data.copy(deep=True)
        #self.series[series_index].data.loc[:,"valor"] = util.linearCombination(self.pivotData(),self.linear_combination,plot=plot)
        self.data.loc[:,"valor"],  self.data.loc[:,"tag"] = util.linearCombination(self.pivotData(),self.linear_combination,plot=plot,tag_column="tag")
    def applyMovingAverage(self):
        for serie in self.series:
            if isinstance(serie,NodeSerie) and serie.moving_average is not None:
                serie.applyMovingAverage()
    def adjustProno(self):
        if not self.series_prono or not len(self.series_prono) or not len(self.series) or self.series[0].data is None:
            return
        truth_data = self.series[0].data
        for serie_prono in [x for x in self.series_prono if x.adjust]:
            sim_data = serie_prono.data[serie_prono.data["tag"]=="prono"]
            # serie_prono.original_data = sim_data.copy(deep=True)
            try:
                adj_serie, tags , model = util.adjustSeries(sim_data,truth_data,method="lfit",plot=True,tag_column="tag",title="%s @ %s" % (serie_prono.name, self.name))
            except ValueError:
                logging.warning("No observations found to estimate coefficients. Skipping adjust")
                return
            # self.series[self.adjust_from["sim"]].data["valor"] = adj_serie
            serie_prono.data.loc[:,"valor"] = adj_serie
            serie_prono.data.loc[:,"tag"] = tags
            serie_prono.adjust_results = model
    # def pasteProno(self):
    #     pasted_data = self.data.copy(deep=True)
    #     if not len(self.series_prono):
    #         logging.warn("No series_prono to paste")
    #         return pasted_data
    #     for serie_prono in self.series_prono:
    #         logging.debug("columns: " + ",".join(serie_prono.data.columns) + ";" + ",".join(pasted_data.columns))
    #         pasted_data = util.serieFillNulls(pasted_data,serie_prono.data,tag_column="tag")
    #     return pasted_data
    def setOutputData(self):
        if self.series_output is not None:
            for serie in self.series_output:
                serie.data = self.data[["valor","tag"]]
                serie.applyOffset()
    def uploadData(self,include_prono=False):
        """
        Uploads series_output to a5 API
        """
        if self.series_output is not None:
            if self.series_output[0].data is None:
                self.setOutputData()
            obs_created = []
            for serie in self.series_output:
                obs_list = serie.toList(remove_nulls=True,max_obs_date=None if include_prono else self.max_obs_date if hasattr(self,"max_obs_date") else None) # include_series_id=True)
                try:
                    created = a5.createObservaciones(obs_list,series_id=serie.series_id)
                    obs_created.extend(created)
                except Exception as e:
                    logging.error(str(e))
            return obs_created
        else:
            logging.warning("Missing output series for node #%i, skipping upload" % self.id)
            return []
    def pivotData(self,include_prono=True):
        data = self.series[0].data[["valor",]]
        for serie in self.series:
            if len(serie.data):
                data = data.join(serie.data[["valor",]],how='outer',rsuffix="_%s" % serie.series_id,sort=True)
        if include_prono and self.series_prono is not None and len(self.series_prono):
            for serie in self.series_prono:
                data = data.join(serie.data[["valor",]],how='outer',rsuffix="_prono_%s" % serie.series_id,sort=True)
        del data["valor"]
        return data
    def pivotOutputData(self,include_tag=True):
        columns = ["valor","tag"] if include_tag else ["valor"]
        data = self.series_output[0].data[columns]
        for serie in self.series_output:
            if len(serie.data):
                data = data.join(serie.data[columns],how='outer',rsuffix="_%s" % serie.series_id,sort=True)
        for column in columns:
            del data[column]
        return data
    def seriesToDataFrame(self,pivot=False,include_prono=True):
        if pivot:
            data = self.pivotData(include_prono)
        else:
            data = self.series[0].data[["valor",]]
            data["series_id"] = self.series[0].series_id
            data["timestart"] = data.index
            data.reset_index()
            for i in range(1,len(self.series)-1):
                if len(self.series[i].data):
                    other_data = self.series[i].data[["valor",]]
                    other_data["series_id"] = self.series[i].series_id
                    other_data["timestart"] = other_data.index
                    other_data.reset_index
                    data = data.append(other_data,ignore_index=True)
        return data
    def saveSeries(self,output,format="csv",pivot=False):
        data = self.seriesToDataFrame(pivot=pivot)
        if format=="csv":
            return data.to_csv(output)
        else:
            return json.dump(data.to_dict(orient="records"),output)
    def concatenateProno(self,inline=True,ignore_warmup=True):
        """
        Fills nulls of data with prono 
        
        :param ignore_warmup: if True, ignores prono before last observation
        :type ignore_warmup: bool
        :param inline: if True, saves into self.data, else returns concatenated dataframe
        :type inline: bool
        :returns: dataframe of concatenated data if inline=False, else None
        """
        if self.series_prono is not None and len(self.series_prono) and len(self.series_prono[0].data):
            prono_data = self.series_prono[0].data[["valor","tag"]]
            self.max_obs_date = self.data[~pandas.isna(self.data["valor"])].index.max()
            if ignore_warmup: #self.forecast_timeend is not None and ignore_warmup:
                prono_data = prono_data[prono_data.index > self.max_obs_date]
            data = util.serieFillNulls(self.data,prono_data,extend=True,tag_column="tag")
            if inline:
                self.data = data
            else:
                return data
        else:
            logging.warning("No series_prono data found for node %i" % self.id)
            if not inline:
                return self.data
    def interpolate(self,limit : timedelta=None,extrapolate=False):
        interpolation_limit = int(limit.total_seconds() / self.time_interval.total_seconds()) if limit is not None else self.interpolation_limit 
        logging.info("interpolation limit:%s" % str(interpolation_limit))
        self.data = util.interpolateData(self.data,column="valor",tag_column="tag",interpolation_limit=interpolation_limit,extrapolate=extrapolate)
        
    def saveData(self,output,format="csv"): #,include_prono=False):
        """
        Saves node.data into file
        """
        # data = self.concatenateProno(inline=False) if include_prono else self.data
        if format=="csv":
            return self.data.to_csv(output)
        else:
            return json.dump(self.data.to_dict(orient="records"),output)
    def plot(self):
        data = self.data[["valor",]]
        pivot_series = self.pivotData()
        data = data.join(pivot_series,how="outer")
        plt.figure(figsize=(16,8))
        if self.timeend is not None:
            plt.axvline(x=self.timeend, color="black",label="timeend")
        if self.forecast_timeend is not None:
            plt.axvline(x=self.forecast_timeend, color="red",label="forecast_timeend")
        plt.plot(data)
        plt.legend(data.columns)
        plt.title(self.name if self.name is not None else self.id)



class ObservedNode(Node):
    def __init__(self,params,timestart,timeend,forecast_timeend,plan=None,time_offset=None):
        super().__init__(params,timestart,timeend,forecast_timeend,plan=plan,time_offset=time_offset)
        self.series = [NodeSerie(x) for x in params["series"]]
        self.series_prono = [NodeSerieProno(x) for x in params["series_prono"]] if "series_prono" in params else None
    def loadData(self,timestart,timeend,include_prono=True,forecast_timeend=None):
        logging.debug("Load data for observed node: %i" % (self.id))
        if self.series is not None:
            for serie in self.series:
                serie.loadData(timestart,timeend)
        elif self.derived_from is not None:
            self.series = []
            self.series[0] = util.se
        if include_prono and self.series_prono is not None and len(self.series_prono):
            for serie in self.series_prono:
                if forecast_timeend is not None:
                    serie.loadData(timestart,forecast_timeend)
                else:
                    serie.loadData(timestart,timeend)
        if self.data is None and len(self.series):
            self.data = self.series[0].data
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
    def regularize(self,interpolate=False):
        for serie in self.series:
            serie.regularize(self.timestart,self.timeend,self.time_interval,self.time_offset,self.interpolation_limit,interpolate=interpolate)
        if self.series_prono is not None:
            for serie in self.series_prono:
                if self.forecast_timeend is not None:
                    serie.regularize(self.timestart,self.forecast_timeend,self.time_interval,self.time_offset,self.interpolation_limit,interpolate=interpolate)
                else:
                    serie.regularize(self.timestart,self.timeend,self.time_interval,self.time_offset,self.interpolation_limit,interpolate=interpolate)
    def fillNulls(self,inline=True,fill_value=None):
        """
        Copies data of first series and fills its null values with the other series
        In the end it fills nulls with fill_value. If None, uses self.fill_value
        If inline=True, saves result in self.data
        """
        fill_value = fill_value if fill_value is not None else self.fill_value
        data = self.series[0].data[["valor","tag"]]
        if len(self.series) > 1:
            i = 2
            for serie in self.series[1:]:
                # if last, fills  
                fill_value_this = fill_value if i == len(self.series) else None 
                data = util.serieFillNulls(data,serie.data,fill_value=fill_value_this,tag_column="tag")
                i = i + 1
        else:
            logging.warning("No other series to fill nulls with")
        if inline:
            self.data = data
        else:
            return data

class DerivedNode(Node):
    def __init__(self,params,timestart,timeend,parent,forecast_timeend=None,plan=None,time_offset=None):
        super().__init__(params,timestart,timeend,forecast_timeend,plan=plan,time_offset=time_offset)
        self.series = []
        if "derived_from" in params:
            for serie in self.series_output:
                self.series.append(DerivedNodeSerie({"series_id":serie.series_id, "derived_from": params["derived_from"]},parent))
        elif "interpolated_from" in params:
            for serie in self.series_output:
                self.series.append(DerivedNodeSerie({"series_id":serie.series_id, "interpolated_from": params["interpolated_from"]},parent))
        if "series" in params:
            self.series.extend([NodeSerie(x) for x in params["series"]])
        if "series_prono" in params:
            self.series_prono = [NodeSerieProno(x) for x in params["series_prono"]]
        else:
            self.series_prono = None
    def derive(self):
        self.series[0].derive()
        self.data = self.series[0].data
        self.original_data = self.data.copy(deep=True)
        if hasattr(self.series[0],"max_obs_date"):
            self.max_obs_date = self.series[0].max_obs_date

class DerivedOrigin:
    def __init__(self,params,topology=None):
        self.node_id = params["node_id"]
        self.x_offset = util.interval2timedelta(params["x_offset"]) if isinstance(params["x_offset"],dict) else params["x_offset"]
        self.y_offset = params["y_offset"]
        if topology is not None:
            from_nodes = [x for x in topology.nodes if x.id == self.node_id]
            if not len(from_nodes):
                raise Exception("origin node not found for derived node, id: %i" % self.node_id)
            self.origin = from_nodes[0]
        else:
            self.origin = None

class InterpolatedOrigin:
    def __init__(self,params,topology=None):
        self.node_id_1 = params["node_id_1"]
        self.node_id_2 = params["node_id_2"]
        self.x_offset = {"hours":0} if "x_offset" not in params else util.interval2timedelta(params["x_offset"]) if isinstance(params["x_offset"],dict) else params["x_offset"]
        self.y_offset = params["y_offset"] if "y_offset" in params else 0
        self.interpolation_coefficient = params["interpolation_coefficient"]
        if topology is not None:
            from_nodes = [x for x in topology.nodes if x.id == self.node_id_1]
            if not len(from_nodes):
                raise Exception("origin node not found for interpolated node, id: %i" % self.interpolated_from.node_id_1)
            self.origin_1 = from_nodes[0]
            from_nodes = [x for x in topology.nodes if x.id == self.node_id_2]
            if not len(from_nodes):
                raise Exception("origin node not found for interpolated node, id: %i" % self.node_id_2)
            self.origin_2 = from_nodes[0]
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


class Topology():
    def __init__(self,params,plan=None):
        jsonschema.validate(instance=params,schema=schema)
        self.timestart = util.tryParseAndLocalizeDate(params["timestart"])
        self.timeend = util.tryParseAndLocalizeDate(params["timeend"])
        self.forecast_timeend = util.tryParseAndLocalizeDate(params["forecast_timeend"]) if "forecast_timeend" in params else None
        self.time_offset_start = util.interval2timedelta(params["time_offset_start"]) if "time_offset_start" in params else util.interval2timedelta(params["time_offset"]) if "time_offset" in params else timedelta(hours=0)
        self.time_offset_end = util.interval2timedelta(params["time_offset_end"]) if "time_offset_end" in params else util.interval2timedelta(params["time_offset"]) if "time_offset" in params else timedelta(hours=datetime.now().hour)
        self.timestart = self.timestart.replace(hour=0,minute=0,second=0,microsecond=0) + self.time_offset_start
        self.timeend = self.timeend.replace(hour=0,minute=0,second=0,microsecond=0) + self.time_offset_end
        if self.timestart >= self.timeend:
            raise("Bad timestart, timeend parameters. timestart must be before timeend")
        self.interpolation_limit = None if "interpolation_limit" not in params else util.interval2timedelta(params["interpolation_limit"]) if isinstance(params["interpolation_limit"],dict) else params["interpolation_limit"]
        self.nodes = []
        for x in params["nodes"]:
            self.nodes.append(DerivedNode(x,self.timestart,self.timeend,self,self.forecast_timeend,plan=plan,time_offset=self.time_offset_start) if "derived" in x and x["derived"] == True else ObservedNode(x,self.timestart,self.timeend,self.forecast_timeend,plan=plan,time_offset=self.time_offset_start))
        self.cal_id = params["cal_id"] if "cal_id" in params else None
    def addNode(self,node,plan=None):
        self.nodes.append(DerivedNode(node,self.timestart,self.timeend,self,self.forecast_timeend,plan=plan,time_offset=self.time_offset_start) if "derived" in node and node["derived"] == True else ObservedNode(node,self.timestart,self.timeend,self.forecast_timeend,plan=plan,time_offset=self.time_offset_start))
    def batchProcessInput(self,include_prono=False):
        logging.debug("loadData")
        self.loadData()
        logging.debug("removeOutliers")
        self.removeOutliers()
        logging.debug("detectJumps")
        self.detectJumps()
        logging.debug("applyOffset")
        self.applyOffset()
        logging.debug("regularize")
        self.regularize()
        logging.debug("applyMovingAverage")
        self.applyMovingAverage()
        logging.debug("fillNulls")
        self.fillNulls()
        logging.debug("adjust")
        self.adjust()
        if include_prono:
            logging.debug("concatenateProno")
            self.concatenateProno()
        logging.debug("derive")
        self.derive()
        logging.debug("interpolate")
        self.interpolate(limit=self.interpolation_limit)
        self.setOutputData()
    def loadData(self,include_prono=True):
        for node in self.nodes:
            if hasattr(node,"loadData"):
                node.loadData(self.timestart,self.timeend,forecast_timeend=self.forecast_timeend,include_prono=include_prono)
            # for serie in node.series:
            #     if isinstance(serie,NodeSerie):
            #         serie.loadData(self.timestart,self.timeend)
                # if include_prono and node.series_prono is not None and len(node.series_prono):
                #     for serie in node.series_prono:
                #         if isinstance(serie,NodeSerieProno):
                #             if self.forecast_timeend is not None:
                #                 serie.loadData(self.timestart,self.forecast_timeend)
                #             else:
                #                 serie.loadData(self.timestart,self.timeend)
            # if isinstance(node,observedNode):
            #     node.loadData(self.timestart,self.timeend)
    def removeOutliers(self):
        found_outliers = False
        for node in self.nodes:
            if isinstance(node,ObservedNode):
                found_outliers_ = node.removeOutliers()
                found_outliers = found_outliers_ if found_outliers_ else found_outliers
        return found_outliers
    def detectJumps(self):
        found_jumps = False
        for node in self.nodes:
            if isinstance(node,ObservedNode):
                found_jumps_ = node.detectJumps()
                found_jumps = found_jumps_ if found_jumps_ else found_jumps
        return found_jumps
    def applyMovingAverage(self):
        for node in self.nodes:
            node.applyMovingAverage()
    def applyOffset(self):
        for node in self.nodes:
            if isinstance(node,ObservedNode):
                node.applyOffset()
    def regularize(self,interpolate=False):
        for node in self.nodes:
            if isinstance(node,ObservedNode):
                node.regularize(interpolate=interpolate)
    def fillNulls(self):
        for node in self.nodes:
            if isinstance(node,ObservedNode):
                node.fillNulls()
    def derive(self):
        for node in self.nodes:
            if isinstance(node,DerivedNode):
                node.derive()
    def adjust(self):
        for node in self.nodes:
            if node.adjust_from is not None:
                node.adjust()
            elif node.linear_combination is not None:
                node.apply_linear_combination()
            node.adjustProno()
    def concatenateProno(self):
        for node in self.nodes:
            if node.series_prono is not None:
                node.concatenateProno()
    def interpolate(self,limit=None):
        for node in self.nodes:
            node.interpolate(limit=limit)
    def setOutputData(self):
        for node in self.nodes:
            node.setOutputData()
    def toCSV(self,pivot=False):
        if pivot:
            data = self.pivotData()
            data["timestart"] = [x.isoformat() for x in data.index]
            # data.reset_index(inplace=True)
            return data.to_csv(index=False)    
        header = ",".join(["timestart","valor","tag","series_id"])
        return header + "\n" + "\n".join([node.toCSV(True,False) for node in self.nodes])
    def outputToCSV(self,pivot=False):
        if pivot:
            data = self.pivotOutputData()
            data["timestart"] = [x.isoformat() for x in data.index]
            # data.reset_index(inplace=True)
            return data.to_csv(index=False)    
        header = ",".join(["timestart","valor","tag","series_id"])
        return header + "\n" + "\n".join([node.outputToCSV(False) for node in self.nodes])
    def toSeries(self,use_node_id=False):
        """
        returns list of Series objects. Same as toList(flatten=True)
        """
        return self.toList(use_node_id=use_node_id,flatten=False)
    def toList(self,pivot=False,use_node_id=False,flatten=True):
        """
        returns list of all data in nodes[0..n].data
        
        pivot: boolean              pivot observations on index (timestart)
        use_node_id: boolean    uses node.id as series_id instead of node.output_series[0].id
        flatten: boolean        if set to false, returns list of series objects:[{"series_id":int,observaciones:[obs,obs,...]},...] (ignored if pivot=True)
        """
        if pivot:
            data = self.pivotData()
            data["timestart"] = [x.isoformat() for x in data.index]
            data.reset_index
            data["timeend"] = data["timestart"]
            return data.to_dict(orient="records")
        obs_list = []
        for node in self.nodes:
            if flatten:
                obs_list.extend(node.toList(True,use_node_id=use_node_id))
            else:
                obs_list.append(node.toSerie(True,use_node_id=use_node_id))
        return obs_list
    def outputToList(self,pivot=False,flatten=False):
        if pivot:
            data = self.pivotOutputData()
            data["timestart"] = [x.isoformat() for x in data.index]
            data.reset_index
            data["timeend"] = data["timestart"]
            data = data.replace({np.nan:None})
            return data.to_dict(orient="records")
        obs_list = []
        for node in self.nodes:
            obs_list.extend(node.outputToList(flatten=flatten))
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
    def saveOutputData(self,file : str,format="csv",pivot=False):
        f = open(file,"w")
        if format == "json":
            obs_json = json.dumps(self.outputToList(pivot),ensure_ascii=False)
            f.write(obs_json)
            f.close()
            return
        f.write(self.outputToCSV(pivot))
        f.close
        return
    def uploadData(self):
        """
        Uploads analysis data of all nodes
        """
        created = []
        for node in self.nodes:
            obs_created = node.uploadData()
            created.extend(obs_created)
        return created
    def uploadDataAsProno(self):
        if self.cal_id is None:
            raise Exception("Missing required parameter cal_id")
        prono = {
            "cal_id": self.cal_id,
            "forecast_date": self.timeend.isoformat(),
            "series": []
        }
        for node in self.nodes:
            serieslist = node.outputToList(flatten=False)
            for serie in serieslist:
                serie["pronosticos"] = serie["observaciones"]
                del serie["observaciones"]
            prono["series"].extend(serieslist)
        return a5.createCorrida(prono)

    def pivotData(self,include_tag=True,use_output_series_id=True,use_node_id=False,nodes=None):
        if nodes is None:
            nodes = self.nodes
        columns = ["valor","tag"] if include_tag else ["valor"]
        data = nodes[0].data[columns]
        for node in nodes:
            if node.data is not None and len(node.data):
                rsuffix = "_%i" % node.series_output[0].series_id if use_output_series_id and node.series_output is not None else "_%s" % str(node.id) if use_node_id else "_%s" % node.name 
                # if include_prono:
                #     node_data = node.concatenateProno(inline=False)
                #     data = data.join(node_data[columns][node_data.valor.notnull()],how='outer',rsuffix=rsuffix,sort=True) # data.join(node.series[0].data[["valor",]][node.series[0].data.valor.notnull()],how='outer',rsuffix="_%s" % node.name,sort=True)    
                # else:
                data = data.join(node.data[columns][node.data.valor.notnull()],how='outer',rsuffix=rsuffix,sort=True) # data.join(node.series[0].data[["valor",]][node.series[0].data.valor.notnull()],how='outer',rsuffix="_%s" % node.name,sort=True)
                if (not use_output_series_id or node.series_output is None) and use_node_id:
                    data = data.rename(columns={"valor_%s" % str(node.id): node.id})
        for column in columns:
            del data[column]
        data = data.replace({np.NaN:None})
        return data
    def pivotOutputData(self,include_tag=True):
        i = 0
        data = None
        for node in self.nodes:
            i = i+1
            node_data = node.pivotOutputData(include_tag=include_tag)
            data = node_data if i == 1 else pandas.concat([data,node_data],axis=1)
        # data = data.replace({np.NaN:None})
        return data
    def plotNodes(self,timestart:datetime=None,timeend:datetime=None):
        for node in self.nodes:
            # if hasattr(node.series[0],"data"):
            if node.data is not None and len(node.data):
                data = node.data.reset_index() # .plot(y="valor")
                if timestart is not None:
                    data = data[data["timestart"] >= timestart]
                if timeend is not None:
                    data = data[data["timestart"] <= timeend]
                # data = node.series[0].data.reset_index() # .plot(y="valor")
                ax = data.plot(kind="scatter",x="timestart",y="valor",title=node.name, figsize=(20,8),grid=True)
                # data.plot.line(x="timestart",y="valor",ax=ax)
                if hasattr(node,"max_obs_date"):
                    ax.axvline(node.max_obs_date, color='k', linestyle='--')
        plt.show()
    # def __iter__(self):
    #     return BordeSetIterator(self)
