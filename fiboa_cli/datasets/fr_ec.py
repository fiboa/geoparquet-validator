import re
import pandas as pd

from ..convert_utils import convert as convert_

# File to read the data from
# Can read any tabular data format that GeoPandas can read through read_file()
# Supported protcols: HTTP(S), GCS, S3, or the local file system

# This one doesn't work because the files in the zip are in a folder, not at the root.
# URI = "https://zenodo.org/records/8229128/files/FR_2018.zip"

# This one doesn't work because the the convert utils doesn't use read_parquet (and most
# fiona installations won't be built against the parquet library as its not in the wheels).
# I tried to change the convert utils to use read_parquet, but it didn't work, at least
# not with this (admittedly large) dataset.
# URI = "https://data.source.coop/cholmes/eurocrops/geoparquet-projected/FR_2018_EC21.parquet"

# This one should work, but it's a 6 gig file, so it'll take a while to download.
URI = "https://data.source.coop/cholmes/eurocrops/unprojected/flatgeobuf/FR_2018_EC21.fgb"

# Unique identifier for the collection
ID = "fr_ec"
# Title of the collection
TITLE = "Field boundaries for France from Eurocrops (2018)"
# Description of the collection. Can be multiline and include CommonMark.
DESCRIPTION = """
This dataset contains the field boundaries for all of France in 2018. The data was collected by the French government and harmonized
by the Eurocrops project and is available on Zenodo.

EuroCrops is a dataset collection combining all publicly available self-declared crop reporting datasets from countries of the European Union.
The project is funded by the German Space Agency at DLR on behalf of the Federal Ministry for Economic Affairs and Climate Action (BMWK).
The work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License][cc-by-sa].

The project developed a new **Hierarchical Crop and Agriculture Taxonomy (HCAT)** that harmonises all declared crops across the European Union. 
In the data you'll find this as additional attributes:

- `EC_trans_n`: The original crop name translated into English
- `EC_hcat_n`: The machine-readable HCAT name of the crop
- `EC_hcat_c`: The 10-digit HCAT code indicating the hierarchy of the crop
"""
# Bounding box of the data in WGS84 coordinates
BBOX = [-6.047022416643922, -3.916364769838749, 68.89050422648864, 51.075100624023094] 

# Provider name, can be None if not applicable, must be provided if PROVIDER_URL is provided
PROVIDER_NAME = "Eurocrops"
# URL to the homepage of the data or the provider, can be None if not applicable
PROVIDER_URL = "https://github.com/maja601/EuroCrops/wiki/France"
# Attribution, can be None if not applicable
ATTRIBUTION = "Institut National de l'Information Géographique et Forestière"

# License of the data, either
# 1. a SPDX license identifier (including "dl-de/by-2-0" / "dl-de/zero-2-0"), or
LICENSE = "CC-BY-2.0"
# 2. a STAC Link Object with relation type "license"
# LICENSE = {'title': 'CC-BY-4.0', 'href': 'https://creativecommons.org/licenses/by/4.0/', 'type': 'text/html', 'rel': 'license'}

# Map original column names to fiboa property names
COLUMNS = {
    'geometry': 'geometry', #fiboa core field
    'ID_PARCEL': 'id', #fiboa core field
    'SURF_PARC': 'area', #fiboa core field
    'CODE_CULTU': 'code_culture', #fiboa custom field
    'CODE_GROUP': 'code_group', #fiboa custom field
    'EC_trans_n': 'EC_trans_n', #fiboa custom field
    'EC_hcat_n': 'EC_hcat_n', #fiboa custom field
    'EC_hcat_c': 'EC_hcat_c' #fiboa custom field
}

# A list of implemented extension identifiers
EXTENSIONS = []

# Functions to migrate data in columns to match the fiboa specification.
# Example: You have a column area_m in square meters and want to convert
# to hectares as required for the area field in fiboa.
# Function signature:
#   func(column: pd.Series) -> pd.Series
COLUMN_MIGRATIONS = {}

ADD_COLUMNS = {
  "determination_datetime": "2018-01-15T00:00:00Z"
}


# Filter columns to only include the ones that are relevant for the collection,
# e.g. only rows that contain the word "agriculture" but not "forest" in the column "land_cover_type".
# Lamda function accepts a Pandas Series and returns a Series or a Tuple with a Series and True to inverse the mask.
COLUMN_FILTERS = {
}

# Custom function to migrate the GeoDataFrame if the other options are not sufficient
# This should be the last resort!
# Function signature:
#   func(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame
MIGRATION = None

# Schemas for the fields that are not defined in fiboa
# Keys must be the values from the COLUMNS dict, not the keys
MISSING_SCHEMAS = {
    'required': ['code_culture', 'code_group', 'EC_trans_n','EC_hcat_n', 'EC_hcat_c'], # i.e. non-nullable properties
    'properties': {
        'code_culture': {
            'type': 'string'
        },
        'code_group': {
            'type': 'uint16'
        },
        'EC_trans_n': {
            'type': 'string'
        },
        'EC_hcat_n': {
            'type': 'string'
        },
        'EC_hcat_c': {
            'type': 'uint32'
        }
    }
}


# Conversion function, usually no changes required
def convert(output_file, cache_file = None, source_coop_url = None, collection = False, compression = None):
    """
    Converts the field boundary datasets to fiboa.

    For reference, this is the order in which the conversion steps are applied:
    0. Read GeoDataFrame from file
    1. Run global migration (if provided through MIGRATION)
    2. Run filters to remove rows that shall not be in the final data
       (if provided through COLUMN_FILTERS)
    3. Run column migrations (if provided through COLUMN_MIGRATIONS)
    4. Duplicate columns (if an array is provided as the value in COLUMNS)
    5. Rename columns (as provided in COLUMNS)
    6. Remove columns (if column is not present as value in COLUMNS)
    7. Create the collection
    8. Change data types of the columns based on the provided schemas
    (fiboa spec, extensions, and MISSING_SCHEMAS)
    9. Write the data to the Parquet file

    Parameters:
    output_file (str): Path where the Parquet file shall be stored.
    cache_file (str): Path to a cached file of the data. Default: None.
                      Can be used to avoid repetitive downloads from the original data source.
    source_coop_url (str): URL to the (future) Source Cooperative repository. Default: None
    collection (bool): Additionally, store the collection separate from Parquet file. Default: False
    kwargs: Additional keyword arguments for GeoPandas read_file() function.
    """
    convert_(
        output_file,
        cache_file,
        URI,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        BBOX,
        provider_name=PROVIDER_NAME,
        provider_url=PROVIDER_URL,
        source_coop_url=source_coop_url,
        extensions=EXTENSIONS,
        missing_schemas=MISSING_SCHEMAS,
        column_migrations=COLUMN_MIGRATIONS,
        column_filters=COLUMN_FILTERS,
        column_additions=ADD_COLUMNS,
        migration=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression
    )
