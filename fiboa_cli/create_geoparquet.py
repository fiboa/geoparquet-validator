import os

from .parquet import create_parquet
from .util import log, load_file

def create_geoparquet(config):
    output_file = config.get("out")

    # Add a STAC collection to the fiboa property to the Parquet metadata
    collection = load_file(config.get("collection"))
    if "id" not in collection or not collection["id"]:
        collection["id"] = os.path.basename(output_file)
    if "fiboa_version" not in collection:
        raise Exception("No fiboa_version found in collection metadata")
    else:
        config["fiboa_version"] = collection["fiboa_version"]
    # todo: fill with more/better metadata

    # Load all features from the GeoJSON files
    features = []
    files = config.get("files")
    for file in files:
        geojson = load_file(file)
        if geojson["type"] == "Feature":
            features.append(geojson)
        elif geojson["type"] == "FeatureCollection":
            features += geojson["features"]
        else:
            log(f"{file}: Skipped - Unsupported GeoJSON type, must be Feature or FeatureCollection")

    if len(features) == 0:
        raise Exception("No valid features provided as input files")

    # Get a list of the properties/columns (without duplicates)
    columns = set(["id", "geometry"])
    for feature in features:
        keys = feature["properties"].keys()
        columns.update(keys)

    columns = list(columns)
    columns.sort()

    # Create the Parquet file
    create_parquet(features, columns, collection, output_file, config)
    log(f"Wrote to {output_file}", "success")
