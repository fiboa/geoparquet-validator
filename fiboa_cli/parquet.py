import json
import pyarrow as pa

from geopandas import GeoDataFrame
from shapely.geometry import shape

from .const import PA_TYPE_MAP, GP_TYPE_MAP, GP_TO_PA_TYPE_MAP
from .util import log, load_fiboa_schema, load_file, merge_schemas
from .geopandas import to_parquet

def create_parquet(data, columns, collection, output_file, config, missing_schemas = {}, compression = "brotli"):
    # Load the data schema
    fiboa_schema = load_fiboa_schema(config)
    schemas = merge_schemas(missing_schemas, fiboa_schema)

    # Load all extension schemas
    extensions = {}
    if "fiboa_extensions" in collection and isinstance(collection["fiboa_extensions"], list):
        ext_map = config.get("extension_schemas", [])
        for ext in collection["fiboa_extensions"]:
            try:
                if ext in ext_map:
                    path = ext_map[ext]
                    log(f"Redirecting {ext} to {path}", "info")
                else:
                    path = ext
                extensions[ext] = load_file(path)
                schemas = merge_schemas(schemas, extensions[ext])
            except Exception as e:
                log(f"Extension schema for {ext} can't be loaded: {e}", "warning")

    # Create GeoDataFrame from the features
    if not isinstance(data, GeoDataFrame):
        data = features_to_dataframe(data, columns)

    # Update the GeoDataFrame with the correct types etc.
    data = update_dataframe(data, columns, schemas)

    # Define the fields for the schema
    pq_fields = []
    for name in columns:
        properties = schemas.get("properties", {})
        if name in properties:
            prop_schema = properties[name]
            pa_type = create_type(prop_schema)
            nullable = name not in schemas.get("required", [])
            field = pa.field(name, pa_type, nullable = nullable)
        else:
            pd_type = str(data[name].dtype) # pandas data type
            pa_type = GP_TO_PA_TYPE_MAP.get(pd_type, None) # pyarrow data type
            if pa_type is not None:
                log(f"{name}: No schema defined, converting {pd_type} to nullable {pa_type}", "warning")
                field = pa.field(name, pa_type, nullable = True)
            else:
                log(f"{name}: No schema defined and converter doesn't support {pd_type}, skipping field", "warning")
                continue

        pq_fields.append(field)

    # Define the schema for the Parquet file
    pq_schema = pa.schema(pq_fields)
    pq_schema = pq_schema.with_metadata({"fiboa": json.dumps(collection).encode("utf-8")})

    # Write the data to the Parquet file
    # Proprietary function exported from geopandas to solve
    # https://github.com/geopandas/geopandas/issues/3182
    to_parquet(
        data,
        output_file,
        schema = pq_schema,
        index = False,
        coerce_timestamps = "ms",
        compression = compression
    )

    return pq_fields


def features_to_dataframe(features, columns):
    # Create a list of shapes
    rows = []
    for feature in features:
        id = feature["id"] if "id" in feature else None
        geometry = shape(feature["geometry"]) if "geometry" in feature else None
        row = {
            "id": id,
            "geometry": geometry,
        }
        properties = feature["properties"] if "properties" in feature else {}
        row.update(properties)
        rows.append(row)

    # Create the GeoDataFrame
    return GeoDataFrame(rows, columns=columns, geometry="geometry", crs="EPSG:4326")


def update_dataframe(data, columns, schema):
    # Convert the data to the correct types
    for column in columns:
        if column not in schema["properties"]:
            continue
        dtype = schema["properties"][column].get("type", None)
        if dtype == "geometry":
            continue

        gp_type = GP_TYPE_MAP.get(dtype, None)
        if gp_type is None:
            log(f"{column}: No type conversion available for {dtype}")
        elif callable(gp_type):
            data[column] = gp_type(data[column])
        else:
            data[column] = data[column].astype(gp_type)

    return data

def create_type(schema):
    dtype = schema.get("type", None)
    if dtype is None:
        raise Exception("No type specified")

    pa_type = PA_TYPE_MAP.get(dtype, None)
    if pa_type is None:
        raise Exception(f"{dtype} is not supported yet")
    elif callable(pa_type):
        if dtype == "array":
            pa_subtype = create_type(schema["items"])
            pa_type = pa_type(pa_subtype)
        elif dtype == "object":
            log(f"Creation of object-typed properties not supported yet", "warning")
            pass # todo

    return pa_type
