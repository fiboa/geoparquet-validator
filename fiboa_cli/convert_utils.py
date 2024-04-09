from .version import fiboa_version
from .util import log, download_file, get_fs, to_iso8601
from .create_geoparquet import create_geoparquet

from fsspec.implementations.local import LocalFileSystem

import os
import json
import geopandas as gpd
import pandas as pd

STAC_TABLE_EXTENSION = "https://stac-extensions.github.io/table/v1.2.0/schema.json"

def convert(
        output_file, cache_file,
        url, columns,
        id, title, description, bbox,
        provider_name = None,
        provider_url = None,
        source_coop_url = None,
        extensions = [],
        missing_schemas = {},
        attribution = None,
        store_collection = False,
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

    collection = create_collection(
        gdf,
        id, title, description, bbox,
        provider_name = provider_name,
        provider_url = provider_url,
        source_coop_url = source_coop_url,
        extensions = extensions,
        attribution = attribution,
        license = license
    )

    log("Creating GeoParquet file: " + output_file)
    config = {
        "fiboa_version": fiboa_version,
    }
    columns = list(actual_columns.values())
    pq_fields = create_geoparquet(gdf, columns, collection, output_file, config, missing_schemas, compression)

    if store_collection:
        external_collection = add_asset_to_collection(collection, output_file, rows = len(gdf), columns = pq_fields)
        collection_file = os.path.join(os.path.dirname(output_file), "collection.json")
        log("Creating Collection file: " + collection_file)
        with open(collection_file, "w") as f:
            json.dump(external_collection, f, indent=2)

    log("Finished", "success")


def create_collection(
        gdf,
        id, title, description, bbox,
        provider_name = None,
        provider_url = None,
        source_coop_url = None,
        extensions = [],
        attribution = None,
        license = "dl-de/by-2-0"
    ):
    """
    Creates a collection for the field boundary datasets.
    """
    if "determination_datetime" not in gdf.columns:
        raise ValueError("determination_datetime column not available")

    dates = pd.to_datetime(gdf['determination_datetime'])
    min_time = to_iso8601(dates.min())
    max_time = to_iso8601(dates.max())

    collection = {
        "fiboa_version": fiboa_version,
        "fiboa_extensions": extensions,
        "stac_version": "1.0.0",
        "type": "Collection",
        "id": id,
        "title": title,
        "description": description,
        "license": "proprietary",
        "providers": [],
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

    # Add providers
    if provider_name is not None:
        collection["providers"].append({
            "name": provider_name,
            "roles": ["producer", "licensor"],
            "url": provider_url
        })

    collection["providers"].append({
        "name": "fiboa CLI",
        "roles": ["processor"],
        "url": "https://pypi.org/project/fiboa-cli"
    })

    if source_coop_url is not None:
        collection["providers"].append({
            "name": "Source Cooperative",
            "roles": ["host"],
            "url": source_coop_url
        })

    # Update attribution
    if attribution is not None:
        collection["attribution"] = attribution

    # Update license
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


def add_asset_to_collection(collection, output_file, rows = None, columns = None):
    c = collection.copy()
    if "assets" not in c or not isinstance(c["assets"], dict):
        c["assets"] = {}
    if "stac_extensions" not in c or not isinstance(c["stac_extensions"], list):
        c["stac_extensions"] = []

    c["stac_extensions"].append(STAC_TABLE_EXTENSION)

    table_columns = []
    for column in columns:
        table_columns.append({
            "name": column.name,
            "type": str(column.type)
        })

    asset = {
        "href": os.path.basename(output_file),
        "title": "Field Boundaries",
        "type": "application/vnd.apache.parquet",
        "roles": [
            "data"
        ],
        "table:columns": table_columns,
        "table:primary_geometry": "geometry"
    }
    if rows is not None:
        asset["table:row_count"] = rows

    c["assets"]["data"] = asset

    return c
