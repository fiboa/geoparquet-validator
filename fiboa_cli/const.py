import pyarrow as pa
import pyarrow.types as pat
import pandas as pd

# fiboa datatypes to geopandas datatypes
# todo: check whether it's better to use nullable Ints (e.g. Int64 instead of int64)
GP_TYPE_MAP = {
    "boolean": "bool",
    "int8": "int8", # todo add enum support - start
    "uint8": "uint8",
    "int16": "int16",
    "uint16": "uint16",
    "int32": "int32",
    "uint32": "uint32",
    "int64": "int64",
    "uint64": "uint64", # todo add enum support - end
    "float": "float32",
    "double": "float64",
    "binary": "bytearray", # todo: check
    "string": "str", # todo: support enum
    "array": None, # todo: object?
    "object": "object",
    "date": "datetime64[D]",
    "date-time": lambda x: pd.to_datetime(x),
    "geometry": None, # not a column, don't convert geometry
    "bounding-box": None # todo
}

# fiboa datatypes to pyarrow datatypes
PA_TYPE_MAP = {
    "boolean": pa.bool_(),
    "int8": pa.int8(), # todo add enum support - start
    "uint8": pa.uint8(),
    "int16": pa.int16(),
    "uint16": pa.uint16(),
    "int32": pa.int32(),
    "uint32": pa.uint32(),
    "int64": pa.int64(),
    "uint64": pa.uint64(), # todo add enum support - end
    "float": pa.float32(),
    "double": pa.float64(),
    "binary": pa.binary(),
    "string": pa.string(), # todo add enum support
    "array": lambda type: pa.list_(type),
    "object": None, # todo: lambda type: pa.map_(pa.string(), type)
    "date": pa.date32(),
    "date-time": pa.timestamp("ms", tz="UTC"),
    "geometry": pa.binary(),
    "bounding-box": None # todo
}

# geopandas datatypes to pyarrow datatypes
GP_TO_PA_TYPE_MAP = {
    "string": pa.string(), # todo add enum support
    "|S0": pa.string(), # todo
    "<U0": pa.string(), # todo
    "bool": pa.bool_(),
    "int8": pa.int8(), # todo add enum support - start
    "uint8": pa.uint8(),
    "int16": pa.int16(),
    "uint16": pa.uint16(),
    "int32": pa.int32(),
    "uint32": pa.uint32(),
    "int64": pa.int64(),
    "uint64": pa.uint64(), # todo add enum support - end
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "float128": None, # todo
    "complex64": None, # todo
    "complex128": None, # todo
    "complex256": None, # todo
    "object": pa.string(),
    "datetime64": pa.timestamp("ms", tz="UTC"),
    "record": None, # todo
    "timedelta64": None # todo
}

# checks pyarrow datatypes
PA_TYPE_CHECK = {
    "boolean": pat.is_boolean,
    "int8": pat.is_int8,
    "uint8": pat.is_uint8,
    "int16": pat.is_int16,
    "uint16": pat.is_uint16,
    "int32": pat.is_int32,
    "uint32": pat.is_uint32,
    "int64": pat.is_int64,
    "uint64": pat.is_uint64,
    "float": pat.is_float32,
    "double": pat.is_float64,
    "binary": pat.is_binary,
    "string": pat.is_string,
    "array": pat.is_list,
    "object": pat.is_map,
    "date": pat.is_date32,
    "date-time": pat.is_timestamp,
    "geometry": pat.is_binary, # todo: check more?
    "bounding-box": None # todo
}

LOG_STATUS_COLOR = {
    "info": "white",
    "warning": "yellow",
    "error": "red",
    "success": "green"
}

SUPPORTED_PROTOCOLS = ["http", "https", "s3", "gs"]

STAC_COLLECTION_SCHEMA = "http://schemas.stacspec.org/v1.0.0/collection-spec/json-schema/collection.json"
