import os

from tempfile import TemporaryDirectory
from zipfile import ZipFile

from ..util import download_file, log
from ..convert_utils import convert as convert_

# File to read the data from
# Can read any tabular data format that GeoPandas can read through read_file()
# Supported protcols: HTTP(S), GCS, S3, or the local file system
URI = "https://zenodo.org/records/10118572/files/FR_2018.zip"
FILENAME = "FR_2018/FR_2018_EC21.shp"

# Unique identifier for the collection
ID = "ec_fr"
# Title of the collection
TITLE = "Field boundaries for France from EuroCrops (2018)"
# Description of the collection. Can be multiline and include CommonMark.
DESCRIPTION = """
This dataset contains the field boundaries for all of France in 2018. The data was collected by the French government and harmonized
by the EuroCrops project and is available on Zenodo.

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
PROVIDER_NAME = "EuroCrops"
# URL to the homepage of the data or the provider, can be None if not applicable
PROVIDER_URL = "https://github.com/maja601/EuroCrops/wiki/France"
# Attribution, can be None if not applicable
ATTRIBUTION = "Institut National de l'Information Géographique et Forestière"

# License of the data, either
LICENSE = "CC-BY-4.0"

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
COLUMN_FILTERS = {}

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
    # We need to extract manually because the GeoPackage is zipped and in a folder, not at the root
    log("Loading file from: " + URI)
    path = download_file(URI, cache_file)

    with TemporaryDirectory() as tmp_dir:
        log("Unzipping to: " + tmp_dir)
        with ZipFile(path, 'r') as f:
            f.extractall(tmp_dir)

        new_path = os.path.join(tmp_dir, FILENAME)

        convert_(
            output_file,
            cache_file,
            new_path,
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
            compression=compression,
        )
