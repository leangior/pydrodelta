import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import os
from pydrodelta.procedure_function import ProcedureFunction, ProcedureFunctionResults
from pathlib import Path
import jsonschema
import yaml

schemas = {}
plan_schema = open("%s/data/schemas/json/polynomialtransformationprocedurefunction.json" % os.environ["PYDRODELTA_DIR"])
schemas["PolynomialTransformationProcedureFunction"] = yaml.load(plan_schema,yaml.CLoader)

base_path = Path("%s/data/schemas/json" % os.environ["PYDRODELTA_DIR"])
resolver = jsonschema.validators.RefResolver(
    base_uri=f"{base_path.as_uri()}/",
    referrer=True,
)

class PolynomialTransformationProcedureFunction(ProcedureFunction):
    def __init__(self,params,procedure):
        super().__init__(params,procedure)
        jsonschema.validate(
            instance=params,
            schema=schemas["PolynomialTransformationProcedureFunction"],
            resolver=resolver)
        self.coefficients = params["coefficients"]
        self.intercept = params["intercept"] if "intercept" in params else 0
    def transformation_function(self,value:float):
        if value is None:
            return None
        result = self.intercept * 1
        exponent = 1
        for c in self.coefficients:
            result = result + value**exponent * c
            exponent = exponent + 1
        return result
    def run(self,input=None):
        if input is None:
            input = self._procedure.loadInput(inline=False,pivot=False)
        output  = []
        for serie in input:
            output_serie = serie.copy()
            output_serie.valor = [self.transformation_function(valor) for valor in output_serie.valor]
            output.append(output_serie)
        return output, ProcedureFunctionResults()