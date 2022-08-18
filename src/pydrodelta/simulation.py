import pydrodelta.analysis as analysis
import jsonschema
import json
# import pydrodelta.Auto_HECRAS as AutoHEC
# import pydrodelta.a5 as a5
import pydrodelta.util as util
import logging
import pydrodelta.hecras as hecras
import os

config_file = open("%s/config/config.json" % os.environ["PYDRODELTA_DIR"]) # "src/pydrodelta/config/config.json")
config = json.load(config_file)
config_file.close()

logging.basicConfig(filename="%s/%s" % (os.environ["PYDRODELTA_DIR"],config["log"]["filename"]), level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
logging.FileHandler("%s/%s" % (os.environ["PYDRODELTA_DIR"],config["log"]["filename"]),"w+")


schemas = {}
plan_schema = open("%s/data/schemas/plan.json" % os.environ["PYDRODELTA_DIR"])
schemas["plan"] = json.load(plan_schema)


class Plan():
    def __init__(self,params):
        jsonschema.validate(params,schemas["plan"])
        self.name = params["name"]
        self.id = params["id"]
        if isinstance(params["topology"],dict):
            self.topology = analysis.Topology(params["topology"])
        else:
            topology_file_path = os.path.join(os.environ["PYDRODELTA_DIR"],params["topology"])
            f = open(topology_file_path)
            self.topology = analysis.Topology(json.load(f),plan=self)
            f.close()
        self.procedures = [createProcedure(x,self) for x in params["procedures"]]
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

def createProcedure(procedure,plan):
    if procedure["type"] in procedureClassDict:
        return procedureClassDict[procedure["type"]](procedure,plan)
    else:
        logging.warn("createProcedure: type %s not found. Instantiating base class Procedure" % procedure["type"])
        return Procedure(procedure,plan)

class Procedure():
    """
    Abstract class to use as base for specific procedures
    """
    def __init__(self,params,plan):
        self.id = params["id"]
        self.plan = plan
        self.input_nodes = []
        for node in params["input_nodes"]:
            if isinstance(node,dict):
                self.input_nodes.append(analysis.Node(node))
            else:
                # str expected, search node identifier of plan.topology
                found_node = False
                for t_node in plan.topology.nodes:
                    if t_node.id == node:
                        self.input_nodes.append(t_node)
                        found_node = True
                        break
                if not found_node:
                    raise("init Procedure error: input node with id: %s not found in topology" % str(node))
        self.output_nodes = []
        for node in params["output_nodes"]:
            if isinstance(node,dict):
                self.output_nodes.append(analysis.Node(node,plan=plan))
            else:
                # str expected, search node identifier of plan.topology
                found_node = False
                for t_node in plan.topology.nodes:
                    if t_node.id == node:
                        self.output_nodes.append(t_node)
                        found_node = True
                        break
                if not found_node:
                    raise("init Procedure error: output node with id: %s not found in topology" % str(node))
        self.initial_states = params["initial_states"] if "initial_states" in params else []
        self.type = params["type"]
        # if self.type in procedureTypeDict:
        #     self.type = procedureTypeDict[self.type]
        # else:
        #     logging.warn("Procedure init: type %s not found. Instantiating base class ProcedureType" % self.type)
        #     self.type = ProcedureType
        # if isinstance(params["procedure_type"],dict):
        #     self.procedure_type = self.type(params["procedure_type"])
        # else:
        #     f = open(params["procedure_type"])
        #     self.procedure_type = self.type(json.load(f))
        #     f.close()
        # self.procedure_type = params["procedure_type"]
        self.parameters = params["parameters"] if "parameters" in params else []
        self.time_interval = util.interval2timedelta(params["time_interval"]) if "time_interval" in params else None
        self.time_offset = util.interval2timedelta(params["time_offset"]) if "time_offset" in params else None
        self.input = None # <- boundary conditions
        self.output = None
        self.states = None
    def loadInput(self):
        self.input = self.plan.topology.pivotData(nodes=self.input_nodes,include_tag=False,use_output_series_id=False,use_node_id=True)
    def run(self,inline=True):
        """
        Placeholder dummy method to be overwritten by specific Procedures

        :param inline: if True, writes output to self.output, else returns output (pivot dataframe)
        """
        data = self.plan.topology.pivotData(nodes=self.output_nodes,include_tag=False,use_output_series_id=False,use_node_id=True)
        if inline:
            self.output = data
        else:
            return data
    def getOutputNodeData(self,node_id,tag=None):
        """
        Extracts single series from pivoted output using node id

        :param node_id: node id
        :returns: timeseries dataframe
        """
        col_rename = {}
        col_rename[node_id] = "valor"
        data = self.output[[node_id]].rename(columns = col_rename)
        if tag is not None:
            data["tag"] = tag
        return data
    def outputToNodes(self):
        if self.output is None:
            logging.error("Procedure output is None, which means the procedure wasn't run yet. Can't perform outputToNodes.")
            return
        output_columns = self.output.columns
        for node in self.output_nodes:
            if node.series_sim is None:
                continue
            if node.id not in output_columns:
                logging.error("Procedure output for node %s not found in self.output. Skipping" % str(node.id))
                continue
            for serie in node.series_sim:
                serie.setData(data=self.getOutputNodeData(node.id,tag=self.id))
                serie.applyOffset()

# class ProcedureType():
#     def __init__(self,params):
#         self.name = params["name"]
#         self.input_names = params["input_names"] if "input_names" in params else []
#         self.output_names = params["output_names"] if "output_names" in params else []
#         self.parameter_names = params["parameter_names"] if "parameter_names" in params else []
#         self.state_names = params["state_names"] if "state_names" in params else []

class QQProcedure(Procedure):
    def __init__(self,params,plan):
        super().__init__(params,plan)

class PQProcedure(Procedure):
    def __init__(self,params,plan):
        super().__init__(params,plan)

class MemProcedure(Procedure):
    def __init__(self,params,plan):
        super().__init__(params,plan)

class HecRasProcedure(Procedure):
    def __init__(self,params,plan):
        super().__init__(params,plan)
        hecras.createHecRasProcedure(self,params,plan)

procedureClassDict = {
    "QQ": QQProcedure,
    "PQ": PQProcedure,
    "Mem": MemProcedure,
    "HecRas":  HecRasProcedure
}

