from pydrodelta.procedure_function import ProcedureFunction, ProcedureFunctionResults
from pydrodelta.validation import getSchema, validate
from pydrodelta.pydrology import MuskingumChannel
from pandas import DataFrame

schemas, resolver = getSchema("MuskingumChannelProcedureFunction","data/schemas/json")
schema = schemas["MuskingumChannelProcedureFunction"]

class MuskingumChannelProcedureFunction(ProcedureFunction):
    def __init__(self,params,procedure):
        """
        Instancia la clase. Lee la configuración del dict params, opcionalmente la valida contra un esquema y los guarda los parámetros y estados iniciales como propiedades de self.
        Guarda procedure en self._procedure (procedimiento al cual pertenece la función)
        """
        super().__init__(params,procedure)
        validate(params,schema,resolver)
        self.K = params["K"]
        self.X = params["X"]
        self.Proc = params["Proc"] if "Proc" in params else "Muskingum"
        self.initial_states = params["initial_states"]

    def run(self,input=None):
        """input[0]: hidrograma en borde superior del tramo (DataFrame con index:timestamp y valor:float)"""
        if input is None:
            input = self._procedure.loadInput(inline=False,pivot=False)
        muskingum_channel = MuskingumChannel([self.K, self.X], input[0]["valor"].to_list(),self.initial_states,self.Proc)
        muskingum_channel.computeOutFlow()
        return [DataFrame({"valor": muskingum_channel.Outflow},index=input[0].index)], ProcedureFunctionResults()