import pydrodelta.analysis as analysis
import jsonschema
import json
# import pydrodelta.Auto_HECRAS as AutoHEC
# import pydrodelta.a5 as a5
import pydrodelta.util as util
import logging
from  pydrodelta.hecras import HecRasProcedureFunction
import os
import yaml
from pydrodelta.procedure_function import ProcedureFunction, ProcedureFunctionResults
from pydrodelta.a5 import createEmptyObsDataFrame
import numpy as np
import click
import sys
from pathlib import Path

config_file = open("%s/config/config.yml" % os.environ["PYDRODELTA_DIR"]) # "src/pydrodelta/config/config.json")
config = yaml.load(config_file,yaml.CLoader)
config_file.close()

logging.basicConfig(filename="%s/%s" % (os.environ["PYDRODELTA_DIR"],config["log"]["filename"]), level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
logging.FileHandler("%s/%s" % (os.environ["PYDRODELTA_DIR"],config["log"]["filename"]),"w+")


schemas = {}
plan_schema = open("%s/data/schemas/json/plan.json" % os.environ["PYDRODELTA_DIR"])
schemas["plan"] = yaml.load(plan_schema,yaml.CLoader)

base_path = Path("%s/data/schemas/json" % os.environ["PYDRODELTA_DIR"])
resolver = jsonschema.validators.RefResolver(
    base_uri=f"{base_path.as_uri()}/",
    referrer=True,
)

class Plan():
    def __init__(self,params):
        jsonschema.validate(
            instance=params,
            schema=schemas["plan"],
            resolver=resolver
        )
        self.name = params["name"]
        self.id = params["id"]
        if isinstance(params["topology"],dict):
            self.topology = analysis.Topology(params["topology"])
        else:
            topology_file_path = os.path.join(os.environ["PYDRODELTA_DIR"],params["topology"])
            f = open(topology_file_path)
            self.topology = analysis.Topology(yaml.load(f,yaml.CLoader),plan=self)
            f.close()
        self.procedures = [Procedure(x,self) for x in params["procedures"]]
    def execute(self,include_prono=True):
        """
        Runs analysis and then each procedure sequentially

        :param include_prono: if True (default), concatenates observed and forecasted boundary conditions. Else, reads only observed data.
        :type include_prono: bool
        :returns: None
        """
        self.topology.batchProcessInput(include_prono=include_prono)
        for procedure in self.procedures:
            procedure.run()

# def createProcedure(procedure,plan):
#     if procedure["type"] in procedureClassDict:
#         return procedureClassDict[procedure["type"]](procedure,plan)
#     else:
#         logging.warn("createProcedure: type %s not found. Instantiating base class Procedure" % procedure["type"])
#         return Procedure(procedure,plan)

# def createProcedure(procedure,plan):
#     if procedure["type"] in procedureClassDict:
#         return procedureClassDict[procedure["type"]](procedure,plan)
#     else:
#         logging.warn("createProcedure: type %s not found. Instantiating base class Procedure" % procedure["type"])
#         return Procedure(procedure,plan)

class ProcedureBoundary():
    """
    A variable at a node which is used as a procedure boundary condition
    """
    def __init__(self,params,plan=None):
        self.node_id = int(params[0])
        self.var_id = int(params[1])
        self.name = str(params[2]) if len(params) > 2 else "%i_%i" % (self.node_id, self.var_id)
        if plan is not None:
            self.setNodeVariable(plan)
        else:
            self._variable = None
            self._node = None
    def setNodeVariable(self,plan):
        for t_node in plan.topology.nodes:
            if t_node.id == self.node_id:
                self._node = t_node
                if self.var_id in t_node.variables:
                    self._variable = t_node.variables[self.var_id]
                    return
        raise("ProcedureBoundary.setNodeVariable error: node with id: %s , var %i not found in topology" % (str(self.node_id), self.var_id))

class Procedure():
    """
    A Procedure defines an hydrological, hydrodinamic or static procedure which takes one or more NodeVariables from the Plan as boundary condition, one or more NodeVariables from the Plan as outputs and a ProcedureFunction. The input is read from the selected boundary NodeVariables and fed into the ProcedureFunction which produces an output, which is written into the output NodeVariables
    """
    def __init__(self,params,plan):
        self.id = params["id"]
        self._plan = plan
        self.boundaries = [ProcedureBoundary(boundary,self._plan) for boundary in params["boundaries"]]
        self.outputs = [ProcedureBoundary(output,self._plan) for output in params["outputs"]]
        self.initial_states = params["initial_states"] if "initial_states" in params else []
        if params["function"]["type"] in procedureFunctionDict:
            self.function_type = procedureFunctionDict[params["function"]["type"]]
        else:
            logging.warn("Procedure init: class %s not found. Instantiating abstract class ProcedureFunction" % params["function"]["type"])
            self.function_type = ProcedureFunction
        # self.function = self.function_type(params["function"])
        if isinstance(params["function"],dict): # read params from dict
            self.function = self.function_type(params["function"],self)
        else: # if not, read from file
            f = open(params["function"])
            self.function = self.function_type(json.load(f),self)
            f.close()
        # self.procedure_type = params["procedure_type"]
        self.parameters = params["parameters"] if "parameters" in params else []
        self.time_interval = util.interval2timedelta(params["time_interval"]) if "time_interval" in params else None
        self.time_offset = util.interval2timedelta(params["time_offset"]) if "time_offset" in params else None
        self.input = None # <- boundary conditions
        self.output = None
        self.states = None
    def loadInput(self,inline=True,pivot=False):
        if pivot:
            data = createEmptyObsDataFrame(extra_columns={"tag":str})
            columns = ["valor","tag"]
            for boundary in self.boundaries:
                if boundary._variable.data is not None and len(boundary._variable.data):
                    rsuffix = "_%s_%i" % (str(boundary.node_id), boundary.var_id) 
                    data = data.join(boundary._variable.data[columns][boundary._variable.data.valor.notnull()],how='outer',rsuffix=rsuffix,sort=True)
            for column in columns:
                del data[column]
            # data = data.replace({np.NaN:None})
        else:
            data = [boundary._variable.data.copy() for boundary in self.boundaries]
        if inline:
            self.input = data
        else:
            return data
    def run(self,inline=True):
        """
        Run self.function.run()

        :param inline: if True, writes output to self.output, else returns output (array of seriesData)
        """
        output = self.function.run()
        if inline:
            self.output = output
        else:
            return output
    def getOutputNodeData(self,node_id,var_id,tag=None):
        """
        Extracts single series from output using node id and variable id

        :param node_id: node id
        :param var_id: variable id
        :returns: timeseries dataframe
        """
        index = 0
        for o in self.outputs:
            if o.var_id == var_id and o.node_id == node_id:
                if self.output is not None and len(self.output) <= index + 1:
                    return self.output[index]
            index = index + 1
        raise("Procedure.getOutputNodeData error: node with id: %s , var %i not found in output" % (str(node_id), var_id))
        # col_rename = {}
        # col_rename[node_id] = "valor"
        # data = self.output[[node_id]].rename(columns = col_rename)
        # if tag is not None:
        #     data["tag"] = tag
        # return data
    def outputToNodes(self):
        if self.output is None:
            logging.error("Procedure output is None, which means the procedure wasn't run yet. Can't perform outputToNodes.")
            return
        # output_columns = self.output.columns
        index = 0
        for o in self.outputs:
            if o._variable.series_sim is None:
                continue
            if index + 1 > len(self.output):
                logging.error("Procedure output for node %s variable %i not found in self.output. Skipping" % (str(o.node_id),o.var_id))
                continue
            for serie in o._variable.series_sim:
                serie.setData(data=self.output[index]) # self.getOutputNodeData(o.node_id,o.var_id))
                serie.applyOffset()
            index = index + 1

# class ProcedureType():
#     def __init__(self,params):
#         self.name = params["name"]
#         self.input_names = params["input_names"] if "input_names" in params else []
#         self.output_names = params["output_names"] if "output_names" in params else []
#         self.parameter_names = params["parameter_names"] if "parameter_names" in params else []
#         self.state_names = params["state_names"] if "state_names" in params else []

# class QQProcedure(Procedure):
#     def __init__(self,params,plan):
#         super().__init__(params,plan)

# class PQProcedure(Procedure):
#     def __init__(self,params,plan):
#         super().__init__(params,plan)

# class MemProcedure(Procedure):
#     def __init__(self,params,plan):
#         super().__init__(params,plan)

# class HecRasProcedure(Procedure):
#     def __init__(self,params,plan):
#         super().__init__(params,plan)
#         hecras.createHecRasProcedure(self,params,plan)

# class LinearProcedure(Procedure):
#     def __init__(self,params,plan):
#         super().__init__(params,plan)

# procedureClassDict = {
#     "QQ": QQProcedure,
#     "PQ": PQProcedure,
#     "Mem": MemProcedure,
#     "HecRas":  HecRasProcedure,
#     "Linear": LinearProcedure
# }


procedureFunctionDict = {
    "ProcedureFunction": ProcedureFunction,
    "HecRas": HecRasProcedureFunction,
    "HecRasProcedureFunction": HecRasProcedureFunction
}


@click.command()
@click.pass_context
@click.argument('config_file', type=str)
@click.option("--csv", "-c", help="Save result of analysis as .csv file", type=str)
@click.option("--json", "-j", help="Save result of analysis to .json file", type=str)
@click.option("--pivot", "-p", is_flag=True, help="Pivot output table", default=False,show_default=True)
@click.option("--upload", "-u", is_flag=True, help="Upload output to database API", default=False, show_default=True)
@click.option("--include_prono", "-P", is_flag=True, help="Concatenate series_prono to output series",type=bool, default=False, show_default=True)
@click.option("--verbose", "-v", is_flag=True, help="log to stdout", default=False, show_default=True)
def run_plan(self,config_file,csv,json,pivot,upload,include_prono,verbose):
    """
    run plan from plan config file
    
    config_file: location of plan config file (.json or .yml)
    """
    if verbose:
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
        handler.setFormatter(formatter)
        root.addHandler(handler)
    t_config = yaml.load(open(config_file),yaml.CLoader)
    plan = Plan(t_config)
    plan.execute(include_prono=include_prono)
    if csv is not None:
        plan.topology.saveData(csv,pivot=pivot)
    if json is not None:
        plan.topology.saveData(json,format="json",pivot=pivot)
    if upload:
        plan.topology.uploadData()
        if include_prono:
            plan.topology.uploadDataAsProno()

