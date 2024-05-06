from .const import STAC_TABLE_EXTENSION
from .version import fiboa_version
from .util import log, download_file, get_fs, to_iso8601
from .parquet import create_parquet

from fsspec.implementations.local import LocalFileSystem

import os
import re
import json
import geopandas as gpd
import pandas as pd

def convert(
        output_file, cache_file,
        url, columns,
        id, title, description, bbox,
        provider_name = None,
        provider_url = None,
        source_coop_url = None,
        extensions = [],
        missing_schemas = {},
        column_additions = {},
        column_filters = {},
        column_migrations = {},
        migration = None,
        attribution = None,
        store_collection = False,
        license = None,
        compression = None,
        **kwargs):
    """
    Converts a German field boundary datasets to fiboa.
    """
    if not isinstance(get_fs(url), LocalFileSystem):
        log("Loading file from: " + url)
    path = download_file(url, cache_file)

    # If file is a parquet file then read with read_parquet
    if path.endswith(".parquet") or path.endswith(".geoparquet"):
        gdf = gpd.read_parquet(path, **kwargs)
    else:
        gdf = gpd.read_file(path, **kwargs)

    log("Loaded into GeoDataFrame:")
    print(gdf.head())

    # 1. Run global migration
    has_migration = callable(migration)
    if has_migration:
        log("Applying global migrations")
        gdf = migration(gdf)
        if not isinstance(gdf, gpd.GeoDataFrame):
            raise ValueError("Migration function must return a GeoDataFrame")

    # 2. Run filters to remove rows that shall not be in the final data
    has_col_filters = len(column_filters) > 0
    if has_col_filters:
        log("Applying filters")
        for key, fn in column_filters.items():
            if key in gdf.columns:
                result = fn(gdf[key])
                # If the result is a tuple, the second value is a flag to potentially invert the mask
                if isinstance(result, tuple):
                    if (result[1]):
                        # Invert mask
                        mask = ~result[0]
                    else:
                        # Use mask as is
                        mask = result[0]
                else:
                    # Just got a mask, proceed
                    mask = result

                # Filter columns based on the mask
                gdf = gdf[mask]
            else:
                log(f"Column '{key}' not found in dataset, skipping filter", "warning")

    # 3. Add constant columns
    has_col_additions = len(column_additions) > 0
    if has_col_additions:
        log("Adding columns")
        for key, value in column_additions.items():
            gdf[key] = value
            columns.append(key)

    # 4. Run column migrations
    has_col_migrations = len(column_migrations) > 0
    if has_col_migrations:
        log("Applying column migrations")
        for key, fn in column_migrations.items():
            if key in gdf.columns:
                gdf[key] = fn(gdf[key])
            else:
                log(f"Column '{key}' not found in dataset, skipping migration", "warning")

    if has_migration or has_col_migrations or has_col_filters or has_col_additions:
        log("GeoDataFrame after migrations and filters:")
        print(gdf.head())

    # 5. Duplicate columns if needed
    actual_columns = {}
    for old_key, new_key in columns.items():
        # If new keys are a list, duplicate the column
        if isinstance(new_key, list):
            for key in new_key:
                gdf[key] = gdf.loc[:, old_key]
                actual_columns[key] = key
        # If new key is a string, plan to rename the column
        elif old_key in gdf.columns:
            actual_columns[old_key] = new_key
        # If old key is not found, remove from the schema and warn
        else:
            log(f"Column '{old_key}' not found in dataset, removing from schema", "warning")

    # 6. Rename columns
    gdf.rename(columns = actual_columns, inplace = True)

    # 7. Remove all columns that are not listed
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
    pq_fields = create_parquet(gdf, columns, collection, output_file, config, missing_schemas, compression)

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
        license = None
    ):
    """
    Creates a collection for the field boundary datasets.
    """
    collection = {
        "fiboa_version": fiboa_version,
        "fiboa_extensions": extensions,
        "type": "Collection",
        "id": id,
        "title": title,
        "description": description,
        "license": "proprietary",
        "providers": [],
        "extent": {
            "spatial": {
                "bbox": [bbox]
            }
        },
        "links": []
    }

    if "determination_datetime" in gdf.columns:
        dates = pd.to_datetime(gdf['determination_datetime'])
        min_time = to_iso8601(dates.min())
        max_time = to_iso8601(dates.max())

        collection["extent"]["temporal"] = {
            "interval": [[min_time, max_time]]
        }
        # Without temporal extent it's not valid STAC
        collection["stac_version"] = "1.0.0"

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
    elif isinstance(license, str):
        if license.lower() == "dl-de/by-2-0":
            collection["links"].append({
                "href": "https://www.govdata.de/dl-de/by-2-0",
                "title": "Data licence Germany - attribution - Version 2.0",
                "type": "text/html",
                "rel": "license"
            })
        elif license.lower() == "dl-de/zero-2-0":
            collection["links"].append({
                "href": "https://www.govdata.de/dl-de/zero-2-0",
                "title": "Data licence Germany - Zero - Version 2.0",
                "type": "text/html",
                "rel": "license"
            })
        elif re.match(r"^[\w-]+$", license):
            collection["license"] = license
        else:
            log(f"Invalid license identifier: {license}", "warning")
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
