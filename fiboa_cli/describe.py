import json
from shapely import wkb
from geopandas import GeoDataFrame

from .util import log, load_parquet_data, load_parquet_schema

def parse_metadata(schema, key):
    if key in schema.metadata:
        return json.loads(schema.metadata[key])
    else:
        log(f"Parquet file schema does not have a '{key}' key", "warning")
        return None


def wkb_to_wkt(data):
    return wkb.loads(data)


def describe(file, display_json=False):
    schema = load_parquet_schema(file)

    log("== GEOPARQUET ==", "success")
    geo = parse_metadata(schema, b"geo")
    geo_columns = []
    if geo:
        log(f"GeoParquet version: {geo['version']}")

        geo_columns = geo.get("columns", {}).keys()
        columns_str = ", ".join(geo_columns)
        log(f"Geometry columns: {columns_str}")

        if (display_json):
            log(json.dumps(geo, indent=2))

    log("\n== COLLECTION ==", "success")
    collection = parse_metadata(schema, b"fiboa")
    if collection:
        log(f"fiboa version: {collection['fiboa_version']}")
        if "fiboa_extensions" in collection and isinstance(collection["fiboa_extensions"], list):
            if len(collection["fiboa_extensions"]) == 0:
                log("fiboa extensions: none")
            else:
                log("fiboa extensions:")
                for ext in collection["fiboa_extensions"]:
                    log(f"  - {ext}")

        if "license" in collection:
            log(f"license: {collection['license']}")

        if (display_json):
            log(json.dumps(collection, indent=2))

    log("\n== SCHEMA ==", "success")
    log(schema.to_string(show_schema_metadata=False))

    data = load_parquet_data(file)
    rowcount = len(data)
    log(f"\n== DATA (rows: {rowcount}) ==", "success")

    geodata = GeoDataFrame(data)
    for col in geo_columns:
        geodata[col] = geodata[col].apply(wkb_to_wkt)
        geodata.set_geometry(col, inplace=True)
    log(geodata.head(10))
