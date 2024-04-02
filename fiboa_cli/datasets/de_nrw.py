from ..util import log, download_file
from ..version import fiboa_version
from ..create import create_parquet
import geopandas as gpd
import pandas as pd

URI = "https://www.opengeodata.nrw.de/produkte/umwelt_klima/bodennutzung/landwirtschaft/LFK-AKTI_EPSG25832_Shape.zip"
COLUMNS = {
    'geometry': 'geometry',
    'ID': 'id',
    'INSPIRE_ID': 'inspire:id',
    'FLIK': 'flik',
    'GUELT_VON': 'determination_datetime',
    'NUTZ_CODE': 'nutz_code',
    'NUTZ_TXT': 'nutz_txt',
    'AREA_HA': 'area'
}

def convert(output_file):
    """
    Converts the DE NRW field boundary datasets to fiboa.
    """
    log("Loading file from: " + URI, "info")
    path = download_file(URI)
    log("Local file is at: " + path, "info")
    gdf = gpd.read_file(path)
    log("Read into Geodataframe:", "info")
    print(gdf.head())

    actual_columns = {}
    for old_key, new_key in COLUMNS.items():
        if old_key in gdf.columns:
            actual_columns[old_key] = new_key
        else:
            log(f"Column '{old_key}' not found in dataset, removing from schema", "warning")

    gdf = gdf.rename(columns = actual_columns)

    log("Creating GeoParquet file: " + output_file, "info")
    collection = create_collection(gdf)
    config = {
        "fiboa_version": fiboa_version,
    }
    columns = list(actual_columns.values())
    create_parquet(gdf, columns, collection, output_file, config)

    log("Finished", "success")


def create_collection(gdf):
    """
    Creates a collection for the DE NRW field boundary datasets.
    """
    dates = pd.to_datetime(gdf['determination_datetime'])
    min_time = dates.min().isoformat() + "Z"
    max_time = dates.max().isoformat() + "Z"

    return {
        "fiboa_version": fiboa_version,
        "fiboa_extensions": [
            "https://fiboa.github.io/inspire-extension/v0.1.0/schema.yaml"
        ],
        "stac_version": "1.0.0",
        "type": "Collection",
        "id": "de_nrw",
        "description": "Field boundaries for North Rhine-Westphalia (NRW), Germany.\nA field block (German: \"Feldblock\") is a contiguous agricultural area surrounded by permanent boundaries, which is cultivated by one or more farmers with one or more crops, is fully or partially set aside or is fully or partially taken out of production. Field blocks are classified separately according to the main land uses of arable land, grassland, permanent crops, 2nd pillar and other. Since 2005, field blocks in NRW have represented the area reference within the framework of the Integrated Administration and Control System (IACS) for EU agricultural subsidies.",
        "license": "proprietary",
        "extent": {
            "spatial": {
                "bbox": [[5.8659988131,50.3226989435,9.4476584861,52.5310351488]]
            },
            "temporal": {
                "interval": [[min_time, max_time]]
            }
        },
        "links": [
            {
                "href": "https://www.govdata.de/dl-de/by-2-0",
                "title": "Data licence Germany - attribution - Version 2.0",
                "type": "text/html",
                "rel": "license"
            }
        ]
    }

