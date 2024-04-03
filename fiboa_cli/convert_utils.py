from .version import fiboa_version
from .util import log, download_file, get_fs
from .create import create_parquet

from fsspec.implementations.local import LocalFileSystem

import geopandas as gpd
import pandas as pd

def convert(
        output_file, cache_file,
        url, columns,
        id, title, description, bbox,
        extensions = [],
        missing_schemas = {},
        attribution = None,
        license = "dl-de/by-2-0",
        compression = "brotli",
        **kwargs):
    """
    Converts a German field boundary datasets to fiboa.
    """
    if not isinstance(get_fs(url), LocalFileSystem):
        log("Loading file from: " + url)
    path = download_file(url, cache_file)

    log("Local file is at: " + path)
    gdf = gpd.read_file(path, **kwargs)

    log("Loaded into GeoDataFrame:")
    print(gdf.head())

    actual_columns = {}
    for old_key, new_key in columns.items():
        # If new keys are a list, duplicate the column
        if isinstance(new_key, list):
            for key in new_key:
                gdf[key] = gdf.loc[:, old_key]
                actual_columns[key] = key
        # If new key is a string, rename the column
        elif old_key in gdf.columns:
            actual_columns[old_key] = new_key
        # If old key is not found, remove from the schema and warn
        else:
            log(f"Column '{old_key}' not found in dataset, removing from schema", "warning")

    # Rename columns
    gdf.rename(columns = actual_columns, inplace = True)
    # Remove all columns that are not listed
    drop_columns = list(set(gdf.columns) - set(actual_columns.values()))
    gdf.drop(columns = drop_columns, inplace = True)

    log("Changed GeoDataFrame to:")
    print(gdf.head())

    log("Creating GeoParquet file: " + output_file)
    collection = create_collection(gdf, id, title, description, bbox, extensions, attribution, license)
    config = {
        "fiboa_version": fiboa_version,
    }
    columns = list(actual_columns.values())
    create_parquet(gdf, columns, collection, output_file, config, missing_schemas, compression)

    log("Finished", "success")

def create_collection(gdf, id, title, description, bbox, extensions = [], attribution = None, license = "dl-de/by-2-0"):
    """
    Creates a collection for the German field boundary datasets.
    """
    if "determination_datetime" not in gdf.columns:
        raise ValueError("determination_datetime column not available")

    dates = pd.to_datetime(gdf['determination_datetime'])
    min_time = dates.min().isoformat() + "Z"
    max_time = dates.max().isoformat() + "Z"

    collection = {
        "fiboa_version": fiboa_version,
        "fiboa_extensions": extensions,
        "stac_version": "1.0.0",
        "type": "Collection",
        "id": id,
        "title": title,
        "description": description,
        "license": "proprietary",
        "extent": {
            "spatial": {
                "bbox": [bbox]
            },
            "temporal": {
                "interval": [[min_time, max_time]]
            }
        },
        "links": []
    }

    if attribution is not None:
        collection["attribution"] = attribution

    if isinstance(license, dict):
        collection["links"].append(license)
    elif license == "dl-de/by-2-0":
        collection["links"].append({
            "href": "https://www.govdata.de/dl-de/by-2-0",
            "title": "Data licence Germany - attribution - Version 2.0",
            "type": "text/html",
            "rel": "license"
        })
    elif license == "dl-de/zero-2-0":
        collection["links"].append({
            "href": "https://www.govdata.de/dl-de/zero-2-0",
            "title": "Data licence Germany - Zero - Version 2.0",
            "type": "text/html",
            "rel": "license"
        })
    elif license == "CC-BY-4.0":
        collection["license"] = license
    else:
        log(f"License information missing", "warning")

    return collection
