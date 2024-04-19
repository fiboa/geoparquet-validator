import json

from .util import log, load_parquet_data, load_parquet_schema, parse_metadata, log_extensions


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
            log_extensions(collection, log)

        if "license" in collection:
            log(f"license: {collection['license']}")

        if (display_json):
            log(json.dumps(collection, indent=2))

    log("\n== SCHEMA ==", "success")
    log(schema.to_string(show_schema_metadata=False))

    geodata = load_parquet_data(file)
    rowcount = len(geodata)
    log(f"\n== DATA (rows: {rowcount}) ==", "success")

    log(geodata.head(10))
