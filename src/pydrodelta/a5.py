from jsonschema import validate as json_validate
import requests
import pandas
import pydrodelta.util as util
import json
import os
from datetime import datetime

config_file = open("%s/config/config.json" % os.environ["PYDRODELTA_DIR"]) # "src/pydrodelta/config/config.json")
config = json.load(config_file)
config_file.close()


schemas = {
    "components":{
        "schemas": {
            "Modelo": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                        "description": "identificador único del modelo"
                    },
                    "nombre": {
                        "type": "string",
                        "description": "Nombre del modelo"
                    },
                    "tipo": {
                        "type": "string",
                        "description": "Tipo de modelo"
                    }
                }
            },
            "ArrayOfModelos": {
                "type": "array",
                "items": {
                    "$ref": "#/components/schemas/Modelo"
                }
            },
            "Calibrado": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único del calibrado"
                    },
                    "model_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único del modelo"
                    },
                    "nombre": {
                        "type": "string",
                        "description": "nombre del calibrado",
                        "format": "regexp"
                    },
                    "activar": {
                        "type": "boolean",
                        "description": "activar el calibrado",
                        "default": True
                    },
                    "outputs": {
                        "type": "array",
                        "items": {
                            "$ref": "#/components/schemas/Output"
                        },
                        "description": "Series de salida del calibrado"
                    },
                    "parametros": {
                        "type": "array",
                        "items": {
                            "$ref": "#/components/schemas/Parametro"
                        },
                        "description": "Parámetros del calibrado"
                    },
                    "estados_iniciales": {
                        "type": "array",
                        "items": {
                            "$ref": "#/components/schemas/Estado"
                        },
                        "description": "Estados iniciales del calibrado"
                    },
                    "forzantes": {
                        "type": "array",
                        "items": {
                            "$ref": "#/components/schemas/Forzante"
                        },
                        "description": "Series de entrada del calibrado"
                    },
                    "selected": {
                        "type": "boolean",
                        "description": "Si el calibrado debe seleccionarse como el principal para las series de salida",
                        "defaultValue": False
                    },
                    "out_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "id de estación de salida del calibrado"
                    },
                    "area_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "id de area del calibrado"
                    },
                    "tramo_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "id de tramo de curso de agua del calibrado"
                    },
                    "dt": {
                        "$ref": "#/components/schemas/TimeInterval",
                        "description": "intervalo temporal del calibrado en formato SQL, p. ej '1 days' o '12 hours' o '00:30:00'",
                        "defaultValue": "1 days"
                    },
                    "t_offset": {
                        "$ref": "#/components/schemas/TimeInterval",
                        "description": "offset temporal del modelo en formato SQL, p ej '9 hours'",
                        "defaultValue": "9 hours"
                    },
                    "grupo_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "id de grupo de calibrados"
                    }
                },
                "required": [
                    "model_id",
                    "nombre"
                ],
                "table_name" : "calibrados"
            },
            "TimeInterval": {
                "oneOf": [
                    {
                        "type": "string",
                        "format": "time-interval"
                    },
                    {
                        "type": "object",
                        "properties": {
                            "milliseconds": {
                                "type": "integer",
                                "format": "int64"
                            },
                            "seconds": {
                                "type": "integer",
                                "format": "int64"
                            },
                            "minutes": {
                                "type": "integer",
                                "format": "int64"
                            },
                            "hours": {
                                "type": "integer",
                                "format": "int64"
                            },
                            "days": {
                                "type": "integer",
                                "format": "int64"
                            },
                            "months": {
                                "type": "integer",
                                "format": "int64"
                            },
                            "years": {
                                "type": "integer",
                                "format": "int64"
                            }
                        }
                    }
                ]
            },
            "Output": {
                "type": "object",
                "properties": {
                    "series_table": {
                        "type": "string",
                        "description": "tabla a la que pertenece la serie de salida",
                        "enum": [
                            "series",
                            "series_areal"
                        ],
                        "defaultValue": "series"
                    },
                    "series_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único de la serie de salida"
                    },
                    "orden": {
                        "type": "integer",
                        "format": "int64",
                        "description": "número de orden de salida",
                        "minimum": 1
                    },
                    "cal_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único de calibrado",
                        "readOnly": True
                    },
                    "id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único",
                        "readOnly": True
                    }
                },
                "required": [
                    "series_id",
                    "orden"
                ],
                "table_name": "cal_out"
            },
            "Parametro": {
                "type": "object",
                "properties": {
                    "valor": {
                        "type": "number",
                        "format": "float",
                        "description": "valor del parámetro"
                    },
                    "orden": {
                        "type": "integer",
                        "format": "int64",
                        "description": "número de orden del parámetro",
                        "minimum": 1
                    },
                    "cal_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único de calibrado",
                        "readOnly": True
                    },
                    "id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único",
                        "readOnly": True
                    }
                },
                "required": [
                    "valor",
                    "orden"
                ],
                "table_name": "cal_pars"
            },
            "Estado": {
                "type": "object",
                "properties": {
                    "valor": {
                        "type": "number",
                        "format": "float",
                        "description": "valor del estado"
                    },
                    "orden": {
                        "type": "integer",
                        "format": "int64",
                        "description": "número de orden del estado",
                        "minimum": 1
                    },
                    "cal_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único de calibrado",
                        "readOnly": True
                    },
                    "id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único",
                        "readOnly": True
                    }
                },
                "required": [
                    "valor",
                    "orden"
                ],
                "table_name": "cal_estados"
            },
            "Forzante": {
                "type": "object",
                "properties": {
                    "series_table": {
                        "type": "string",
                        "description": "tabla a la que pertenece la serie de entrada",
                        "enum": [
                            "series",
                            "series_areal"
                        ],
                        "defaultValue": "series"
                    },
                    "series_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único de la serie de entrada"
                    },
                    "orden": {
                        "type": "integer",
                        "format": "int64",
                        "description": "número de orden de entrada",
                        "minimum": 1
                    },
                    "cal_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único de calibrado",
                        "readOnly": True
                    },
                    "id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único",
                        "readOnly": True
                    },
                    "model_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único de modelo",
                        "readOnly": True
                    },
                    "serie": {
                        "$ref":"#/components/schemas/Serie",
                        "description": "serie: objeto Serie",
                        "foreign_key": "series_id"
                    }
                },
                "required": [
                    "series_id",
                    "orden"
                ],
                "table_name": "forzantes"
            },
            "Corrida": {
                "type": "object",
                "properties": {
                    "forecast_date": {
                        "type": "string",
                        "description": "Fecha de emisión"
                    },
                    "series": {
                        "type": "array",
                        "description": "series temporales simuladas",
                        "items": {
                            "$ref": "#/components/schemas/SerieTemporalSim"
                        }
                    }
                },
                "required": [
                    "forecast_date",
                    "series"
                ]
            },
            "SerieTemporalSim": {
                "type": "object",
                "properties": {
                    "series_table": {
                        "type": "string",
                        "description": "tabla de la serie simulada",
                        "enum": [
                            "series",
                            "series_areal"
                        ],
                        "defaultValue": "series"
                    },
                    "series_id": {
                        "type": "integer",
                        "format": "int64",
                        "description": "identificador único de serie simulada"
                    },
                    "pronosticos": {
                        "type": "array",
                        "items": {
                            "$ref": "#/components/schemas/Pronostico"
                        },
                        "description": "Tuplas de la serie simulada"
                    }
                },
                "required": [
                    "series_table",
                    "series_id",
                    "pronosticos"
                ]
            },
            "Pronostico": {
                "type": "object",
                "properties": {
                    "timestart": {
                        "type": "string",
                        "description": "fecha-hora inicial del pronóstico",
                        "format": "date-time",
                        "interval": "begin"
                    },
                    "timeend": {
                        "type": "string",
                        "description": "fecha-hora final del pronóstico",
                        "format": "date-time",
                        "interval": "end"
                    },
                    "valor": {
                        "type": "number",
                        "format": "float",
                        "description": "valor del pronóstico"
                    },
                    "qualifier": {
                        "type": "string",
                        "description": "calificador opcional para diferenciar subseries, default:'main'",
                        "defaultValue": "main"
                    }
                },
                "required": [
                    "timestart",
                    "timeend",
                    "valor"
                ]
            },
            "Observacion": {
                "type": "object",
                "properties": {
                    "tipo": {
                        "type": "string",
                        "description": "tipo de registro",
                        "enum": [
                            "areal",
                            "puntual",
                            "raster"
                        ]
                    },
                    "timestart": {
                        "type": "string",
                        "description": "fecha-hora inicial del registro"
                    },
                    "timeend": {
                        "type": "string",
                        "description": "fecha-hora final del registro"
                    },
                    "valor": {
                        "oneOf": [
                            {
                                "type": "number",
                                "format": "float"
                            },
                            {
                                "type": "string",
                                "format": "binary"
                            }
                        ],
                        "description": "valor del registro"
                    },
                    "series_id": {
                        "type": "integer",
                        "description": "id de serie"
                    }
                },
                "required": ["timestart", "valor"]
            },
            "Accessor": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "nombre identificador del recurso"
                    },
                    "url": {
                        "type": "string",
                        "description": "ubicación del recurso"
                    },
                    "class": {
                        "type": "string",
                        "description": "tipo de recurso"
                    },
                    "series_tipo": {
                        "type": "string",
                        "enum": [
                            "puntual",
                            "areal",
                            "raster"
                        ],
                        "description": "tipo de la serie temporal correspondiente al recurso"
                    },
                    "series_source_id": {
                        "type": "integer",
                        "description": "id de la fuente correspondiente al recurso"
                    },
                    "time_update": {
                        "type": "string",
                        "description": "última fecha de actualización del recurso"
                    },
                    "config": {
                        "type": "object",
                        "properties": {
                            "download_dir": {
                                "type": "string",
                                "description": "directorio de descargas"
                            },
                            "tmp_dir": {
                                "type": "string",
                                "description": "directorio temporal"
                            },
                            "tables_dir": {
                                "type": "string",
                                "description": "directorio de tablas"
                            },
                            "host": {
                                "type": "string",
                                "description": "IP o url del recurso"
                            },
                            "user": {
                                "type": "string",
                                "description": "nombre de usuario del recurso"
                            },
                            "password": {
                                "type": "string",
                                "description": "contraseña del recurso"
                            },
                            "path": {
                                "type": "string",
                                "description": "ruta del recurso"
                            }
                        }
                    },
                    "series_id": {
                        "type": "integer",
                        "description": "id de la serie temporal correspondiente al recurso"
                    }
                }
            },
            "Fuente": {
                "oneOf": [ 
                    {"$ref": "#/components/schemas/FuenteRaster"},
                    {"$ref": "#/components/schemas/FuentePuntual"}
                ]
            },
            "FuenteRaster": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                        "description": "id de la fuente"
                    },
                    "nombre": {
                        "type": "string",
                        "description": "nombre de la fuente"
                    },
                    "data_table": {
                        "type": "string",
                        "description": ""
                    },
                    "data_column": {
                        "type": "string",
                        "description": ""
                    },
                    "tipo": {
                        "type": "string",
                        "description": "tipo de la fuente"
                    },
                    "def_proc_id": {
                        "type": "string",
                        "description": "id de procedimiento por defecto de la fuente"
                    },
                    "def_dt": {
                        "type": "string",
                        "description": "intervalo temporal por defecto de la fuente"
                    },
                    "hora_corte": {
                        "type": "string",
                        "description": "hora de corte por defecto de la fuente"
                    },
                    "def_unit_id": {
                        "type": "integer",
                        "description": "id de unidades por defecto de la fuente"
                    },
                    "def_var_id": {
                        "type": "integer",
                        "description": "id de variable por defecto de la fuente"
                    },
                    "fd_column": {
                        "type": "string",
                        "description": ""
                    },
                    "mad_table": {
                        "type": "string",
                        "description": ""
                    },
                    "scale_factor": {
                        "type": "number",
                        "description": "factor de escala por defecto de la fuente"
                    },
                    "data_offset": {
                        "type": "number",
                        "description": "offset por defecto de la fuente"
                    },
                    "def_pixel_height": {
                        "type": " number",
                        "description": "altura de pixel por defecto de la fuente"
                    },
                    "def_pixel_width": {
                        "type": "number",
                        "description": "ancho de pixel por defecto de la fuente"
                    },
                    "def_srid": {
                        "type": "integer",
                        "description": "código SRID de georeferenciación por defecto de la fuente"
                    },
                    "def_extent": {
                        "type": "string",
                        "description": "id de procedimiento por defecto de la fuente"
                    },
                    "date_column": {
                        "type": "string",
                        "description": ""
                    },
                    "def_pixel_type": {
                        "type": "string",
                        "description": "tipo de dato del pixel por defecto de la fuente"
                    },
                    "abstract": {
                        "type": "string",
                        "description": "descripción de la fuente"
                    },
                    "source": {
                        "type": "string",
                        "description": "ubicación del origen de la fuente"
                    }
                }
            },
            "FuentePuntual": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                        "description": "id numérico de la fuente"
                    },
                    "tabla_id": {
                        "type": "string",
                        "description": "id alfanumérico de la fuente"
                    },
                    "nombre": {
                        "type": "string",
                        "description": "nombre de la fuente"
                    },
                    "public": {
                        "type": "boolean",
                        "description": "si la fuente es pública"
                    },
                    "public_his_plata": {
                        "type": "boolean",
                        "description": "si la fuente está disponible para HIS-Plata"
                    }
                }
            },
            "Variable": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                        "description": "id de la variable"
                    },
                    "var": {
                        "description": "código alfanumérico de la variable",
                        "type": "string"
                    },
                    "nombre": {
                        "description": "Nombre de la variable",
                        "type": "string"
                    },
                    "abrev": {
                        "description": "Abreviatura de la variable",
                        "type": "string"
                    },
                    "type": {
                        "description": "tipo de la variable",
                        "type": "string"
                    },
                    "dataType": {
                        "description": "tipo de dato de la variable según ODM",
                        "type": "string"
                    },
                    "valueType": {
                        "description": "tipo de valor de la variable según ODM",
                        "type": "string"
                    },
                    "GeneralCategory": {
                        "description": "categoría general de la variable según ODM",
                        "type": "string"
                    },
                    "VariableName": {
                        "description": "nombre de la variable según ODM",
                        "type": "string"
                    },
                    "SampleMedium": {
                        "description": "Medio de muestreo según ODM",
                        "type": "string"
                    },
                    "def_unit_id": {
                        "description": "id de unidades por defecto",
                        "type": "integer"
                    },
                    "timeSupport": {
                        "description": "soporte temporal de la medición",
                        "type": "string"
                    }
                }
            },
            "Procedimiento": {
                "type": "object",
                "properties": {
                    "id": {
                        "description": "id del Procedimiento",
                        "type": "integer"
                    },
                    "nombre": {
                        "description": "Nombre del Procedimiento",
                        "type": "string"
                    },
                    "abrev": {
                        "description": "Nombre abreviado del Procedimiento",
                        "type": "string"
                    },
                    "descripicion": {
                        "description": "descripción del Procedimiento",
                        "type": "string"
                    }
                }
            },
            "Unidad": {
                "type": "object",
                "properties": {
                    "id": {
                        "description": "id de la Unidad",
                        "type": "integer"
                    },
                    "nombre": {
                        "description": "Nombre de la Unidad",
                        "type": "string"
                    },
                    "abrev": {
                        "description": "Nombre abreviado de la Unidad",
                        "type": "string"
                    },
                    "UnitsID": {
                        "description": "ID de unidades según ODM",
                        "type": "string"
                    },
                    "UnitsType": {
                        "description": "tipo de unidades según ODM",
                        "type": "string"
                    }
                }
            },
            "Estacion": {
                "type": "object",
                "properties": {
                    "fuentes_id": {
                        "description": "id de la fuente",
                        "type": "integer"
                    },
                    "nombre": {
                        "description": "nombre de la estación (parcial o completo)",
                        "type": "string"
                    },
                    "unid": {
                        "description": "identificador único de la estación",
                        "type": "integer"
                    },
                    "id": {
                        "description": "identificador de la estación dentro de la fuente (red) a la que pertenece",
                        "type": "integer"
                    },
                    "id_externo": {
                        "description": "id externo de la estación",
                        "type": "string"
                    },
                    "distrito": {
                        "description": "jurisdicción de segundo orden en la que se encuentra la estación (parcial o completa)",
                        "type": "string"
                    },
                    "pais": {
                        "description": "jurisdicción de primer orden en la que se encuentra la estación (parcial o completa)",
                        "type": "string"
                    },
                    "has_obs": {
                        "description": "si la estación posee registros observados",
                        "type": "boolean"
                    },
                    "real": {
                        "name": "real",
                        "type": "boolean"
                    },
                    "habilitar": {
                        "description": "si la estación se encuentra habilitada",
                        "type": "boolean"
                    },
                    "tipo": {
                        "description": "tipo de la estación",
                        "type": "string"
                    },
                    "has_prono": {
                        "description": "si la estación posee registros pronosticados",
                        "type": "boolean"
                    },
                    "rio": {
                        "description": "curso de agua de la estación (parcial o completo)",
                        "type": "string"
                    },
                    "tipo_2": {
                        "description": "tipo de estación: marca y/o modelo",
                        "type": "string"
                    },
                    "geom": {
                        "description": "coordenadas geográficas de la estación",
                        "$ref": "#/components/schemas/Geometry"
                    },
                    "propietario": {
                        "description": "propietario de la estación (nombre parcial o completo)",
                        "type": "string"
                    },
                    "automatica": {
                        "description": "si la estación es automática",
                        "type": "boolean"
                    },
                    "ubicacion": {
                        "description": "ubicación de la estación",
                        "type": "string"
                    },
                    "localidad": {
                        "description": "localidad en la que se encuentra la estación",
                        "type": "string"
                    },
                    "tabla": {
                        "description": "identificación alfanumérica de la fuente (red) a la que pertenece la estación",
                        "type": "string"
                    }
                }
            },
            "Area": {
                "type": "object",
                "properties": {
                    "nombre": {
                        "description": "nombre del área",
                        "type": "string"
                    },
                    "id": {
                        "description": "identificador único del área",
                        "type": "integer"
                    },
                    "geom": {
                        "description": "geometría del área (polígono)",
                        "$ref": "#/components/schemas/Geometry"
                    },
                    "exutorio": {
                        "description": "geometría de la sección de salida (punto)",
                        "$ref": "#/components/schemas/Geometry"
                    }
                }
            },
            "Escena": {
                "type": "object",
                "properties": {
                    "nombre": {
                        "description": "nombre de la escena",
                        "type": "string"
                    },
                    "unid": {
                        "description": "identificador único de la escena",
                        "type": "integer"
                    },
                    "geom": {
                        "description": "geometría de la escena (polígono)",
                        "$ref": "#/components/schemas/Geometry"
                    }
                }
            },
            "Geometry": {
                "type": "object",
                "properties": {
                    "type": {
                        "description": "tipo de geometría",
                        "type": "string",
                        "enum": [ "Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "GeometryCollection" ]
                    },
                    "coordinates": {
                        "description": "coordenadas",
                        "oneOf": [
                            {
                                "$ref": "#/components/schemas/Position"
                            },
                            {
                                "$ref": "#/components/schemas/LineString"
                            },
                            {
                                "$ref": "#/components/schemas/Polygon"
                            },
                            {
                                "$ref": "#/components/schemas/MultiPolygon"
                            }
                        ]
                    }
                },
                "required": [ "type", "coordinates"]
            },
        "Position": {
                "type": "array",
                "items": {
                    "type": "number"
                },
                "minItems": 2,
                "maxItems": 3
            },
            "LineString": {
                "type": "array",
                "items": {
                    "$ref": "#/components/schemas/Position"
                },
                "minItems": 2
            },
            "Polygon": {
                "type": "array",
                "items": {
                    "$ref": "#/components/schemas/LineString"
                }
            },
            "MultiPolygon": {
                "type": "array",
                "items": {
                    "$ref": "#/components/schemas/Polygon"
                }
            },
            "Serie": {
                "type": "object",
                "properties": {
                    "tipo": {
                        "description": "tipo de observación",
                        "type": "string",
                        "enum": [ "puntual", "areal", "raster" ]
                    },
                    "id": {
                        "description": "id de la serie",
                        "type": "integer"
                    },
                    "estacion": {
                        "description": "estación/área/escena (para tipo: puntual/areal/raster respectivamente)",
                        "oneOf": [
                            {
                                "$ref": "#/components/schemas/Estacion"
                            },
                            {
                                "$ref": "#/components/schemas/Area"
                            },
                            {
                                "$ref": "#/components/schemas/Escena"
                            }
                        ],
                        "foreign_key": "estacion_id"
                    },
                    "var": {
                        "description": "variable",
                        "$ref": "#/components/schemas/Variable"
                    },
                    "procedimiento": {
                        "description": "procedimiento",
                        "$ref": "#/components/schemas/Procedimiento",
                        "foreign_key": "proc_id"
                    },
                    "unidades": {
                        "description": "unidades",
                        "$ref": "#/components/schemas/Unidad",
                        "foreign_key": "unit_id"
                    },
                    "fuente": {
                        "description": "fuente (para tipo: areal/raster)",
                        "$ref": "#/components/schemas/Fuente",
                        "foreign_key": "fuentes_id"
                    },
                    "observaciones": {
                        "description": "arreglo de observaciones correspondientes a la serie",
                        "type": "array",
                        "items": {
                            "$ref": "#/components/schemas/Observacion"
                        }
                    }
                },
                "required": [ "tipo", "estacion_id", "var_id", "proc_id", "unit_id"],
                "table_name" : "series"
            },
            "ObservacionDia": {
                "type": "object",
                "properties": {
                    "date": {
                        "description": "fecha de la observación",
                        "type": "string"
                    },
                    "series_id": {
                        "description": "id de serie",
                        "type": "integer"
                    },
                    "var_id": {
                        "description": "id de variable",
                        "type": "integer"
                    },
                    "proc_id": {
                        "description": "id de procedimiento",
                        "type": "integer"
                    },
                    "unit_id": {
                        "description": "id de unidades",
                        "type": "integer"
                    },
                    "estacion_id": {
                        "description": "id de estacion (tipo puntual)",
                        "type": "integer"
                    },
                    "valor": {
                        "description": "valor de la observación",
                        "type": "number"
                    }, 
                    "fuentes_id": {
                        "description": "id de fuente (tipo areal y raster)",
                        "type": "integer"
                    },
                    "area_id":  {
                        "description": "id de area (tipo areal)",
                        "type": "integer"
                    },
                    "tipo": {
                        "description": "tipo de observación",
                        "type": "string",
                        "enum": ["puntual", "areal", "raster"]
                    },
                    "doy": {
                        "description": "día del año",
                        "type": "integer"
                    },
                    "cume_dist": {
                        "description": "valor de distribución acumulada",
                        "type": "number"
                    }
                }
            },
            "Asociacion": {
                "type": "object",
                "properties": {
                    "id":{
                        "type": "integer"
                    },
                    "source_tipo":{
                        "type": "string"
                    },
                    "source_series_id":{
                        "type": "integer"
                    },
                    "dest_tipo":{
                        "type": "string"
                    },
                    "dest_series_id":{
                        "type": "integer"
                    },
                    "agg_func":{
                        "type": "string"
                    },
                    "dt":{
                        "type": "string"
                    },
                    "t_offset": {
                        "type": "string"
                    },
                    "precision":{
                        "type": "integer"
                    }, 
                    "source_time_support":{
                        "type": "string"
                    },
                    "source_is_inst":{
                        "type": "boolean"
                    },
                    "source_series": {
                        "$ref": "#/components/schemas/Serie"
                    },
                    "dest_series": {
                        "$ref": "#/components/schemas/Serie"
                    },
                    "site": {
                        "$ref": "#/components/schemas/Estacion"
                    },
                    "expresion": {
                        "type": "string"
                    }
                }
            },
            "EstadisticosDiarios": {
                "type": "object",
                "properties": {
                    "tipo":{
                        "description": "tipo de observación",
                        "type": "string"
                    },
                    "series_id":{
                        "description": "id de serie",
                        "type": "integer"
                    },
                    "doy":{
                        "description": "día del año",
                        "type": "integer"
                    },
                    "count":{
                        "description": "cantidad de registros",
                        "type": "integer"
                    },
                    "min":{
                        "description": "valor mínimo",
                        "type": "number"
                    },
                    "max":{
                        "description": "valor máximo",
                        "type": "number"
                    },
                    "mean":{
                        "description": "valor medio",
                        "type": "number"
                    },
                    "p01":{
                        "description": "percentil 1",
                        "type": "number"
                    },
                    "p10":{
                        "description": "percentil 10",
                        "type": "number"
                    },
                    "p50":{
                        "description": "percentil 50",
                        "type": "number"
                    },
                    "p90":{
                        "description": "percentil 90",
                        "type": "number"
                    },
                    "p99":{
                        "description": "percentil 99",
                        "type": "number"
                    },
                    "window_size":{
                        "description": "ventana temporal para el suavizado en días (a partir de y hasta el día del año en cuestión)",
                        "type": "integer"
                    },
                    "timestart":{
                        "description": "fecha inicial",
                        "type": "string"
                    },
                    "timeend":{
                        "description": "fecha final",
                        "type": "string"
                    }
                }
            },
            "Percentil": {
                "type": "object",
                "properties": {
                    "tipo":{
                        "type": "string"
                    },
                    "series_id":{
                        "type": "integer"
                    },
                    "cuantil":{
                        "type": "number"
                    },
                    "window_size":{
                        "type": "integer"
                    },
                    "doy":{
                        "type": "integer"
                    },
                    "timestart":{
                        "type": "string"
                    },
                    "timeend":{
                        "type": "string"
                    },
                    "count":{
                        "type": "integer"
                    },
                    "valor":{
                        "type": "number"
                    }
                }
            },
            "GeoJSON": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string"
                    },
                    "features": {
                        "type": "array",
                        "items": {
                            "$ref": "#/components/schemas/Feature"
                        }
                    }
                }
            },
            "Feature": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "number"
                    },
                    "geometry": {
                        "$ref": "#/components/schemas/Geometry"
                    },
                    "properties": {
                        "type": "object"
                    }
                }
            }
        }
    }
}
serie_schema = open("%s/data/schemas/serie.json" % os.environ["PYDRODELTA_DIR"])
serie_schema = json.load(serie_schema)

def validate(instance,classname):
    if classname not in schemas["components"]["schemas"].keys():
        raise Exception("Invalid class")
    return json_validate(instance,schema=schemas) #[classname])

# CLASSES

class Serie():
    def __init__(self,params):
        json_validate(params,schema=serie_schema)
        self.id = params["id"] if "id" in params else None
        self.tipo = params["tipo"] if "tipo" in params else None
        self.observaciones = [Observacion(o) for o in params["observaciones"]] if "observaciones" in params else []
    def toDict(self):
        return {
            "id": self.id,
            "tipo": self.tipo,
            "observaciones": [o.toDict() for o in self.observaciones]
        }

class Observacion():
    def __init__(self,params):
        # json_validate(params,"Observacion")
        self.timestart = params["timestart"] if isinstance(params["timestart"],datetime) else util.tryParseAndLocalizeDate(params["timestart"])
        self.timeend = None if "timeend" not in params else params["timeend"] if isinstance(params["timeend"],datetime) else util.tryParseAndLocalizeDate(params["timeend"])
        self.valor = params["valor"]
        self.series_id = params["series_id"] if "series_id" in params else None
        self.tipo = params["tipo"] if "tipo" in params else "puntual"
        self.tag = params["tag"] if "tag" in params else None
    def toDict(self):
        return {
            "timestart": self.timestart.isoformat(),
            "timeend": self.timeend.isoformat() if self.timeend is not None else None,
            "valor": self.valor,
            "series_id": self.series_id,
            "tipo": self.tipo,
            "tag": self.tag
        }
            
# CRUD

def readSeries(tipo="puntual",series_id=None,area_id=None,estacion_id=None,escena_id=None,var_id=None,proc_id=None,unit_id=None,fuentes_id=None,tabla=None,id_externo=None,geom=None,include_geom=None,no_metadata=None,date_range_before=None,date_range_after=None,getMonthlyStats=None,getStats=None,getPercentiles=None,percentil=None,use_proxy=False):
    if date_range_before is not None:
        date_range_before = date_range_before if isinstance(date_range_before,str) else date_range_before.isoformat()
    if date_range_after is not None:
        date_range_after =date_range_after if isinstance(date_range_after,str) else date_range_after.isoformat()
    params = locals()
    del params["use_proxy"]
    del params["tipo"]
    response = requests.get("%s/obs/%s/series" % (config["api"]["url"], tipo),
        params = params,
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def readSerie(series_id,timestart=None,timeend=None,tipo="puntual",use_proxy=False):
    params = {}
    if timestart is not None and timeend is not None:
        params = {
            "timestart": timestart if isinstance(timestart,str) else timestart.isoformat(),
            "timeend": timeend if isinstance(timeend,str) else timeend.isoformat()
        }
    response = requests.get("%s/obs/%s/series/%i" % (config["api"]["url"], tipo, series_id),
        params = params,
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def observacionesDataFrameToList(data : pandas.DataFrame,series_id : int,column="valor",timeSupport=None):
    # data: dataframe con índice tipo datetime y valores en columna "column"
    # timeSupport: timedelta object
    if data.index.dtype.name != 'datetime64[ns, America/Argentina/Buenos_Aires]':
       data.index = data.index.map(util.tryParseAndLocalizeDate)
       # raise Exception("index must be of type datetime64[ns, America/Argentina/Buenos_Aires]'")
    if column not in data.columns:
        raise Exception("column %s not found in data" % column)
    data = data.sort_index()
    data["series_id"] = series_id
    data["timestart"] = data.index.map(lambda x: x.isoformat()) # strftime('%Y-%m-%dT%H:%M:%SZ') 
    data["timeend"] = data["timestart"] if timeSupport is None else data["timestart"].apply(lambda x: x + timeSupport)
    data["valor"] = data[column]
    data = data[["series_id","timestart","timeend","valor"]]
    return data.to_dict(orient="records")

def observacionesListToDataFrame(data: list, tag: str=None):
    if len(data) == 0:
        raise Exception("empty list")
    data = pandas.DataFrame.from_dict(data)
    data["valor"] = data["valor"].astype(float)
    data.index = data["timestart"].apply(util.tryParseAndLocalizeDate)
    data.sort_index(inplace=True)
    if tag is not None:
        data["tag"] = tag
        return data[["valor","tag"]]
    else:
        return data[["valor",]]

def createEmptyObsDataFrame(extra_columns : dict=None):
    data = pandas.DataFrame({
        "timestart": pandas.Series(dtype='datetime64[ns, America/Argentina/Buenos_Aires]'),
        "valor": pandas.Series(dtype="float")
    })
    cnames = ["valor"]
    if extra_columns is not None:
        for cname in extra_columns:
            data[cname] = pandas.Series(dtype=extra_columns[cname])
            cnames.append(cname)
    data.index = data["timestart"]
    return data [cnames]

def createObservaciones(data,series_id : int,column="valor",tipo="puntual", timeSupport=None,use_proxy=False):
    if isinstance(data,pandas.DataFrame):
        data = observacionesDataFrameToList(data,series_id,column,timeSupport)
    [validate(x,"Observacion") for x in data]
    url = "%s/obs/%s/series/%i/observaciones" % (config["api"]["url"], tipo, series_id) if series_id is not None else "%s/obs/%s/observaciones" % (config["api"]["url"], tipo)
    response = requests.post(url, json = {
            "observaciones": data
        }, headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def createCorrida(data,cal_id=None,use_proxy=False):
    validate(data,"Corrida")
    cal_id = cal_id if cal_id is not None else data["cal_id"] if "cal_id" in data else None
    if cal_id is None:
        raise Exception("Missing parameter cal_id")
    url = "%s/sim/calibrados/%i/corridas" % (config["api"]["url"], cal_id)
    response = requests.post(url, json = data, headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    print(response.url)
    if response.status_code != 200:
        raise Exception("request failed: status: %i, message: %s" % (response.status_code, response.text))
    json_response = response.json()
    return json_response

def readVar(var_id,use_proxy=False):
    response = requests.get("%s/obs/variables/%i" % (config["api"]["url"], var_id),
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def readSerieProno(series_id,cal_id,timestart=None,timeend=None,use_proxy=False,cor_id=None,forecast_date=None,qualifier=None):
    """
    Reads prono serie from a5 API
    if forecast_date is not None, cor_id is overwritten by first corridas match
    returns Corridas object { series_id: int, cor_id: int, forecast_date: str, pronosticos: [{timestart:str,valor:float},...]}
    """
    params = {}
    if forecast_date is not None:
        corridas_response = requests.get("%s/sim/calibrados/%i/corridas" % (config["api"]["url"], cal_id),
            params = {
                "forecast_date": forecast_date if isinstance(forecast_date,str) else forecast_date.isoformat()
            },
            headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
            proxies = config["proxy_dict"] if use_proxy else None
        )
        if corridas_response.status_code != 200:
            raise Exception("request failed: %s" % corridas_response.text)
        corridas = corridas_response.json()
        if len(corridas):
            cor_id = corridas[0]["cor_id"]
        else:
            print("Warning: series %i from cal_id %i at forecast_date %s not found" % (series_id,cal_id,forecast_date))
            return {
            "series_id": series_id,
            "pronosticos": []
        }
    if timestart is not None and timeend is not None:
        params = {
            "timestart": timestart if isinstance(timestart,str) else timestart.isoformat(),
            "timeend": timeend if isinstance(timestart,str) else timeend.isoformat(),
            "series_id": series_id
        }
    if qualifier is not None:
        params["qualifier"] = qualifier
    url = "%s/sim/calibrados/%i/corridas/last" % (config["api"]["url"], cal_id)
    if cor_id is not None:
        url = "%s/sim/calibrados/%i/corridas/%i" % (config["api"]["url"], cal_id, cor_id)
    response = requests.get(url,
        params = params,
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    if "series" not in json_response:
        print("Warning: series %i from cal_id %i not found" % (series_id,cal_id))
        return {
            "series_id": series_id,
            "pronosticos": []
        }
    if not len(json_response["series"]):
        print("Warning: series %i from cal_id %i not found" % (series_id,cal_id))
        return {
            "series_id": series_id,
            "pronosticos": []
        }
    if "pronosticos" not in json_response["series"][0]:
        print("Warning: pronosticos from series %i from cal_id %i not found" % (series_id,cal_id))
        return {
            "series_id": series_id,
            "pronosticos": []
        }
    if not len(json_response["series"][0]["pronosticos"]):
        print("Warning: pronosticos from series %i from cal_id %i is empty" % (series_id,cal_id))
        return json_response["series"][0]
    json_response["series"][0]["pronosticos"] = [ { "timestart": x[0], "valor": x[2]} for x in json_response["series"][0] ["pronosticos"]] # "series_id": series_id, "timeend": x[1] "qualifier":x[3]
    return json_response["series"][0]

## EJEMPLO
'''
import pydrodelta.a5 as a5
import pydrodelta.util as util
# lee serie de api a5
serie = a5.readSerie(31532,"2022-05-25T03:00:00Z","2022-06-01T03:00:00Z")
serie2 = a5.readSerie(26286,"2022-05-01T03:00:00Z","2022-06-01T03:00:00Z")
# convierte observaciones a dataframe 
obs_df = a5.observacionesListToDataFrame(serie["observaciones"]) 
obs_df2 = a5.observacionesListToDataFrame(serie["observaciones"]) 
# crea index regular
new_index = util.createRegularDatetimeSequence(obs_df.index,timedelta(days=1))
# crea index regular a partir de timestart timeend
timestart = util.tryParseAndLocalizeDate("1989-10-14T03:00:00.000Z")
timeend = util.tryParseAndLocalizeDate("1990-03-10T03:00:00.000Z")
new_index=util.createDatetimeSequence(timeInterval=timedelta(days=1),timestart=timestart,timeend=timeend,timeOffset=timedelta(hours=6))
# genera serie regular
reg_df = util.serieRegular(obs_df,timeInterval=timedelta(hours=12))
reg_df2 = util.serieRegular(obs_df2,timeInterval=timedelta(hours=12),interpolation_limit=1)
# rellena nulos con otra serie
filled_df = util.serieFillNulls(reg_df,reg_df2)
# convierte de dataframe a lista de dict
obs_list = a5.observacionesDataFrameToList(obs_df,series_id=serie["id"])
# valida observaciones
for x in obs_list:
    a5.validate(x,"Observacion")
# sube observaciones a la api a5
upserted = a5.createObservaciones(obs_df,series_id=serie["id"])
'''