import os

import pandas as pd

from .parquet import create_parquet
from .util import load_parquet_data, load_parquet_schema, log, parse_metadata
from .version import fiboa_version

DEFAULT_COLUMNS = [
    "id",
    "geometry",
    "area",
    "perimeter",
    "determination_datetime",
    "determination_method",
]
DEFAULT_CRS = "EPSG:4326"

def merge(datasets, out, crs = DEFAULT_CRS, includes = [], excludes = [], extensions = [], compression = None, geoparquet1 = False):
    dir = os.path.dirname(out)
    if dir:
        os.makedirs(dir, exist_ok=True)

    columns = DEFAULT_COLUMNS.copy()
    columns.extend(includes)
    columns = list(set(columns) - set(excludes))

    # Load the datasets
    all_gdf = []
    for dataset in datasets:
        # Load the dataset
        schema = load_parquet_schema(dataset)
        file_columns = list(set(columns) & set(schema.names))
        gdf = load_parquet_data(dataset, columns=file_columns)

        # Change the CRS if necessary
        gdf.to_crs(crs=crs, inplace=True)

        # Add collection column to each dataset
        collection = parse_metadata(schema, b"fiboa")
        if collection is not None and "id" in collection:
            gdf["collection"] = collection["id"]
        else:
            gdf["collection"] = os.path.splitext(os.path.basename(dataset))[0]

        all_gdf.append(gdf)

    merged = pd.concat(all_gdf, ignore_index=True)

    # Remove empty columns
    merged.dropna(axis=1, how='all', inplace=True)
    columns = list(merged.columns)
    columns.sort()

    # Create collection metadata
    collection = {
        "fiboa_version": fiboa_version,
        "fiboa_extensions": extensions,
    }

    # Write the merged dataset to the output file
    create_parquet(merged, columns, collection, out, {}, compression=compression, geoparquet1=geoparquet1)
    log(f"Merged data to {out}", "success")
