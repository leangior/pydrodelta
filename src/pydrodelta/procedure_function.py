class ProcedureFunction:
    """
    Abstract class to represent the transformation function of the procedure. It is instantiation, 'params' should be a dictionary which may contain an array of numerical or string 'parameters', an array of numerical or string 'initial_states', whereas 'procedure' must be the Procedure element which contains the function. The .run() method should accept an optional array of seriesData as 'input' and return an array of seriesData and a procedureFunctionResults object. When extending this class, any additional parameters may be added to 'params'.
    """
    def __init__(self,params,procedure):
        self.parameters = params["parameters"] if "parameters" in params else []
        self.initial_states = params["initial_states"] if "initial_states" in params else []
        self._procedure = procedure
    def run(self,input=None):
        """
        Placeholder dummy method to be overwritten by specific Procedures

        :param input: array of seriesData
        :returns [seriesData], procedureFunctionResults
        """
        if input is None:
            input = self._procedure.loadInput(inline=False)
        # data = self._plan.topology.pivotData(nodes=self.output_nodes,include_tag=False,use_output_series_id=False,use_node_id=True)
        return input, ProcedureFunctionResults({"init_states": input})

class ProcedureFunctionResults:
    def __init__(self,params:dict={}):
        self.init_states = params["init_states"] if "init_states" in params else None
        self.states = params["states"] if "states" in params else None
        self.parameters = params["parameters"] if "parameters" in params else None
        self.statistics = params["statistics"] if "statistics" in params else None
