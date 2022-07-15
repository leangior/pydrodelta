import pydrodelta.a5 as a5
import pydrodelta.util as util
from datetime import timedelta 
import json
import numpy as np
import matplotlib.pyplot as plt
import logging

config_file = open("config.json")
config = json.load(config_file)
config_file.close()

logging.basicConfig(filename=config["log"]["filename"], level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
logging.FileHandler(config["log"]["filename"],"w+")

class NodeSerie():
    def __init__(self,params):
        self.series_id = params["series_id"]
        self.lim_outliers = params["lim_outliers"]
        self.lim_jump = params["lim_jump"]
        self.x_offset = util.interval2timedelta(params["x_offset"]) if isinstance(params["x_offset"],dict) else params["x_offset"] # shift_by
        self.y_offset = params["y_offset"]  # bias
        self.moving_average = util.interval2timedelta(params["moving_average"]) if "moving_average" in params else None
        self.data = None
    def loadData(self,timestart,timeend):
        logging.debug("Load data for series_id: %i" % (self.series_id))
        self.data = a5.readSerie(self.series_id,timestart,timeend)
        if len(self.data["observaciones"]):
            self.data = a5.observacionesListToDataFrame(self.data["observaciones"])
        else:
            logging.warning("No data found for series_id=%i" % self.series_id)
            self.data = a5.createEmptyObsDataFrame()
        self.original_data = self.data.copy(deep=True)
    def removeOutliers(self):
        self.outliers_data = util.removeOutliers(self.data,self.lim_outliers)
        if len(self.outliers_data):
            return True
        else:
            return False
    def detectJumps(self):
        self.jumps_data = util.detectJumps(self.data,self.lim_jump)
        if len(self.jumps_data):
            return True
        else:
            return False
    def applyMovingAverage(self):
        if self.moving_average is not None:
            self.data["valor"] = util.serieMovingAverage(self.data,self.moving_average)
    def applyOffset(self):
        if isinstance(self.x_offset,timedelta):
            self.data.index = [x + self.x_offset for x in self.data.index]
        elif self.x_offset != 0:
            self.obs["valor"] = self.obs["valor"].shift(self.x_offset, axis = 0) 
        if self.y_offset != 0:
            self.data["valor"] = self.data["valor"] + self.y_offset
    def regularize(self,timestart,timeend,time_interval,time_offset):
        self.data = util.serieRegular(self.data,time_interval,timestart,timeend,time_offset)
    def fillNulls(self,other_data,fill_value=None,x_offset=0,y_offset=0,inline=False):
        data = util.serieFillNulls(self.data,other_data,fill_value=fill_value,shift_by=x_offset,bias=y_offset)
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
    def toList(self,include_series_id=False,timeSupport=None):
        data = self.data
        data["timestart"] = data.index
        data["timeend"] = [x + timeSupport for x in data["timestart"]] if timeSupport is not None else data["timestart"]
        data["timestart"] = [x.isoformat() for x in data["timestart"]]
        data["timeend"] = [x.isoformat() for x in data["timeend"]]
        if include_series_id:
            data["series_id"] = self.series_id
        return data.to_dict(orient="records")

class NodeSerieProno(NodeSerie):
    def __init__(self,params):
        super().__init__(params)        
        self.cal_id = params["cal_id"]
        self.cor_id = None
        self.forecast_date = None
    def loadData(self,timestart,timeend):
        logging.debug("Load prono data for series_id: %i, cal_id: %i" % (self.series_id, self.cal_id))
        self.data = a5.readSerieProno(self.series_id,self.cal_id,timestart,timeend)
        if len(self.data["pronosticos"]):
            self.data = a5.observacionesListToDataFrame(self.data["pronosticos"])
        else:
            logging.warning("No data found for series_id=%i, cal_id=%i" % (self.series_id, self.cal_id))
            self.data = a5.createEmptyObsDataFrame()
        self.original_data = self.data.copy(deep=True)

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
    def derive(self,keep_index=True):
        if self.derived_from is not None:
            logging.debug("Deriving %i from %s" % (self.series_id, self.derived_from.origin.name))
            self.data = self.derived_from.origin.data[["valor",]] # self.derived_from.origin.series[0].data[["valor",]]
            if isinstance(self.derived_from.x_offset,timedelta):
                self.data["valor"] = self.data["valor"] + self.derived_from.y_offset
                self.data.index = [x + self.derived_from.x_offset for x in self.data.index]
            else:
                self.data["valor"] = self.data["valor"].shift(self.derived_from.x_offset, axis = 0) + self.derived_from.y_offset
        elif self.interpolated_from is not None:
            logging.debug("Interpolating %i from %s and %s" % (self.series_id, self.interpolated_from.origin_1.name, self.interpolated_from.origin_2.name))
            self.data = self.interpolated_from.origin_1.data[["valor",]] # self.interpolated_from.origin_1.series[0].data[["valor",]]
            self.data = self.data.join(self.interpolated_from.origin_2.data[["valor",]],how='left',rsuffix="_other") # self.data.join(self.interpolated_from.origin_2.series[0].data[["valor",]],how='left',rsuffix="_other")
            self.data["valor"] = self.data["valor"] * (1 - self.interpolated_from.interpolation_coefficient) + self.data["valor_other"] * self.interpolated_from.interpolation_coefficient
            del self.data["valor_other"]
            if isinstance(self.interpolated_from.x_offset,timedelta):
                if keep_index:
                    self.data = util.applyTimeOffsetToIndex(self.data,self.interpolated_from.x_offset)
                else:
                    self.data.index = [x + self.interpolated_from.x_offset for x in self.data.index]
            else:
                self.data["valor"] = self.data["valor"].shift(self.interpolated_from.x_offset, axis = 0)    
            
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
    def __init__(self,params,timestart=None,timeend=None,forecast_timeend=None):
        if "id" not in params:
            raise ValueError("id of node must be defined")
        self.id = params["id"]
        if "name" not in params:
            raise ValueError("name of node must be defined")
        self.name = params["name"]
        self.timestart = timestart
        self.timeend = timeend
        self.forecast_timeend = forecast_timeend
        if "time_interval" not in params:
            raise ValueError("time_interval of node must be defined")
        self.time_interval = util.interval2timedelta(params["time_interval"])
        self.time_offset = util.interval2timedelta(params["time_offset"]) if "time_offset" in params and params["time_offset"] is not None else None
        self.fill_value = params["fill_value"] if "fill_value" in params else None
        self.output_series_id = params["output_series_id"] if "output_series_id" in params else None
        self.time_support = util.interval2timedelta(params["time_support"]) if "time_support" in params else None 
        self.adjust_from = params["adjust_from"] if "adjust_from" in params else None
        self.linear_combination = params["linear_combination"] if "linear_combination" in params else None
        self.data = None
        self.original_data = None
    def toCSV(self,include_series_id=False,include_header=True):
        if include_series_id:
            data = self.data[["valor",]] # self.series[0].data
            data["series_id"] = self.output_series_id
            return data.to_csv(header=include_header)
        return self.data.to_csv(header=include_header) # self.series[0].toCSV()
    def toList(self,include_series_id=False):
        data = self.data[self.data.valor.notnull()] # self.series[0].data[self.series[0].data.valor.notnull()]
        data.loc[:,"timestart"] = data.index
        data.loc[:,"timeend"] = [x + self.time_support for x in data["timestart"]] if self.time_support is not None else data["timestart"]
        data.loc[:,"timestart"] = [x.isoformat() for x in data["timestart"]]
        data.loc[:,"timeend"] = [x.isoformat() for x in data["timeend"]]
        if include_series_id:
            data.loc[:,"series_id"] = self.output_series_id
        return data.to_dict(orient="records")
    def adjust(self,plot=True):
        truth_data = self.series[self.adjust_from["truth"]].data
        sim_data = self.series[self.adjust_from["sim"]].data
        self.series[self.adjust_from["sim"]].original_data = sim_data.copy(deep=True)
        try:
            adj_serie = util.adjustSeries(sim_data,truth_data,method=self.adjust_from["method"],plot=plot)
        except ValueError:
            logging.warning("No observations found to estimate coefficients. Skipping adjust")
            return
        # self.series[self.adjust_from["sim"]].data["valor"] = adj_serie
        self.data.loc[:,"valor"] = adj_serie
    def apply_linear_combination(self,plot=True,series_index=0):
        self.series[series_index].original_data = self.series[series_index].data.copy(deep=True)
        #self.series[series_index].data.loc[:,"valor"] = util.linearCombination(self.pivotData(),self.linear_combination,plot=plot)
        self.data.loc[:,"valor"] = util.linearCombination(self.pivotData(),self.linear_combination,plot=plot)
    def applyMovingAverage(self):
        for serie in self.series:
            if isinstance(serie,NodeSerie) and serie.moving_average is not None:
                serie.applyMovingAverage()
    def uploadData(self):
        obs_list = self.toList()
        if self.output_series_id is not None:
            obs_created = a5.createObservaciones(obs_list,series_id=self.output_series_id)
            return obs_created
        else:
            logging.warning("Missing output_series_id for node #%i, skipping upload" % self.id)
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
    def seriesToDataFrame(self,pivot=False):
        if pivot:
            data = self.pivotData()
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
        If ignore_warmup=True, ignores prono before self.timeend
        If inline=True, saves into self.data, else returns concatenated dataframe
        """
        if self.series_prono is not None and len(self.series_prono) and len(self.series_prono[0].data):
            prono_data = self.series_prono[0].data[["valor",]]
            if self.forecast_timeend is not None and ignore_warmup:
                prono_data = prono_data[prono_data.index >= self.timeend]
            data = util.serieFillNulls(self.data,prono_data,extend=True)
            if inline:
                self.data = data
            else:
                return data
        else:
            logging.warning("No series_prono data found for node %i" % self.id)
            if not inline:
                return self.data
    def saveData(self,output,format="csv",include_prono=False):
        """
        Saves node.data into file
        """
        if include_prono:
            data = self.concatenateProno(inline=False)
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
    def __init__(self,params,timestart,timeend,forecast_timeend):
        super().__init__(params,timestart,timeend,forecast_timeend)
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
    def regularize(self):
        for serie in self.series:
            serie.regularize(self.timestart,self.timeend,self.time_interval,self.time_offset)
        if self.series_prono is not None:
            for serie in self.series_prono:
                if self.forecast_timeend is not None:
                    serie.regularize(self.timestart,self.forecast_timeend,self.time_interval,self.time_offset)
                else:
                    serie.regularize(self.timestart,self.timeend,self.time_interval,self.time_offset)
    def fillNulls(self,inline=True,fill_value=None):
        """
        Copies data of first series and fills its null values with the other series
        In the end it fills nulls with fill_value. If None, uses self.fill_value
        If inline=True, saves result in self.data
        """
        fill_value = fill_value if fill_value is not None else self.fill_value
        data = self.series[0].data[["valor",]]
        if len(self.series) > 1:
            i = 2
            for serie in self.series[1:]:
                # if last, fills  
                fill_value_this = fill_value if i == len(self.series) else None 
                data = util.serieFillNulls(data,serie.data,fill_value=fill_value_this)
                i = i + 1
        else:
            logging.warning("No other series to fill nulls with")
        if inline:
            self.data = data
        else:
            return data

class derivedNode(Node):
    def __init__(self,params,timestart,timeend,parent,forecast_timeend=None):
        super().__init__(params,timestart,timeend,forecast_timeend)
        if "series" in params:
            self.series = [NodeSerie(x) for x in params["series"]]
        else:
            self.series = []
        if "series_prono" in params:
            self.series_prono = [NodeSerieProno(x) for x in params["series_prono"]]
        else:
            self.series_prono = None
        if "derived_from" in params:
            self.series.append(DerivedNodeSerie({"series_id":self.output_series_id, "derived_from": params["derived_from"]},parent))
        elif "interpolated_from" in params:
            self.series.append(DerivedNodeSerie({"series_id":self.output_series_id, "interpolated_from": params["interpolated_from"]},parent))
    def derive(self):
        self.series[0].derive()
        self.data = self.series[0].data
        self.original_data = self.data.copy(deep=True)

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
        self.x_offset = util.interval2timedelta(params["x_offset"]) if isinstance(params["x_offset"],dict) else params["x_offset"]
        self.y_offset = params["y_offset"]
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
    def __init__(self,params):
        self.timestart = util.tryParseAndLocalizeDate(params["timestart"])
        self.timeend = util.tryParseAndLocalizeDate(params["timeend"])
        self.forecast_timeend = util.tryParseAndLocalizeDate(params["forecast_timeend"]) if "forecast_timeend" in params else None
        self.nodes = []
        for x in params["nodes"]:
            self.nodes.append(derivedNode(x,self.timestart,self.timeend,self,self.forecast_timeend) if "derived" in x and x["derived"] == True else ObservedNode(x,self.timestart,self.timeend,self.forecast_timeend))
    def addNode(self,node):
        self.nodes.append(derivedNode(node,self.timestart,self.timeend,self,self.forecast_timeend) if "derived" in node and node["derived"] == True else ObservedNode(node,self.timestart,self.timeend,self.forecast_timeend))
    def batchProcessInput(self):
        self.loadData()
        self.removeOutliers()
        self.detectJumps()
        self.applyOffset()
        self.regularize()
        self.applyMovingAverage()
        self.fillNulls()
        self.derive()
        self.adjust()
    def loadData(self,include_prono=True):
        for node in self.nodes:
            if hasattr(node,"loadData"):
                node.loadData(self.timestart,self.timeend,forecast_timeend=self.forecast_timeend)
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
    def regularize(self):
        for node in self.nodes:
            if isinstance(node,ObservedNode):
                node.regularize()
    def fillNulls(self):
        for node in self.nodes:
            if isinstance(node,ObservedNode):
                node.fillNulls()
    def derive(self):
        for node in self.nodes:
            if isinstance(node,derivedNode):
                node.derive()
    def adjust(self):
        for node in self.nodes:
            if node.adjust_from is not None:
                node.adjust()
            elif node.linear_combination is not None:
                node.apply_linear_combination()
    def toCSV(self,pivot=False):
        if pivot:
            data = self.pivotData()
            data["timestart"] = [x.isoformat() for x in data.index]
            data.reset_index
            return data.to_csv(index=False)    
        header = ",".join(["timestart","valor","series_id"])
        return header + "\n" + "\n".join([node.toCSV(True,False) for node in self.nodes])
    def toList(self,pivot=False):
        if pivot:
            data = self.pivotData()
            data["timestart"] = [x.isoformat() for x in data.index]
            data.reset_index
            data["timeend"] = data["timestart"]
            return data.to_dict(orient="records")
        obs_list = []
        for node in self.nodes:
            obs_list.extend(node.toList(True))
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
        """
        Uploads analysis data of all nodes
        """
        created = []
        for node in self.nodes:
            obs_created = node.uploadData()
            created.extend(obs_created)
        return created
    def pivotData(self):
        data = self.nodes[0].data[["valor",]] # self.nodes[0].series[0].data[["valor",]]
        for node in self.nodes:
            if node.data is not None and len(node.data): # len(node.series) and series[0].data):
                data = data.join(node.data[["valor",]][node.data.valor.notnull()],how='outer',rsuffix="_%s" % node.name,sort=True) # data.join(node.series[0].data[["valor",]][node.series[0].data.valor.notnull()],how='outer',rsuffix="_%s" % node.name,sort=True)
        del data["valor"]
        data = data.replace({np.NaN:None})
        return data
    def plotNodes(self):
        for node in self.nodes:
            # if hasattr(node.series[0],"data"):
            if node.data is not None and len(node.data):
                data = node.data.reset_index() # .plot(y="valor")
                # data = node.series[0].data.reset_index() # .plot(y="valor")
                data.plot(kind="scatter",x="timestart",y="valor",title=node.name)
        plt.show()
    # def __iter__(self):
    #     return BordeSetIterator(self)
