from ..convert_utils import convert as convert_
import re
import pandas as pd

# File to read the data from
# Can read any tabular data format that GeoPandas can read through read_file()
# Supported protcols: HTTP(S), GCS, S3, or the local file system
SOURCES = {
     "https://zenodo.org/records/6868143/files/LT_2021.zip": ["LT/LT_2021_EC.shp"]
}


# Unique identifier for the collection
ID = "ec_lt"
# Title of the collection
TITLE = "Field boundaries for Lithuania (Eurocrops, 2021)"
# Description of the collection. Can be multiline and include CommonMark.
DESCRIPTION = """
Collection of data on agricultural land and crop areas, cultivated crops in the territory of the Republic of Lithuania.

The download service is a set of personalized spatial data of agricultural land and crop areas, cultivated crops. The service provides object geometry with descriptive (attributive) data. 
"""

# Provider name, can be None if not applicable, must be provided if PROVIDER_URL is provided
PROVIDER_NAME = "Eurocrops"
# URL to the homepage of the data or the provider, can be None if not applicable

# original dataset : https://www.geoportal.lt/geoportal/nacionaline-mokejimo-agentura-prie-zemes-ukio-ministerijos#savedSearchId={772172A4-6719-48BD-8DDC-5DEEFB27DE74}&collapsed=true
PROVIDER_URL = "https://github.com/maja601/EuroCrops/wiki/Lithuania"
# Attribution, can be None if not applicable
ATTRIBUTION = "National Paying Agency under the Ministry of Agriculture"


# https://www.geoportal.lt/metadata-catalog/catalog/search/resource/details.page?uuid=%7B7AF3F5B2-DC58-4EC5-916C-813E994B2DCF%7D
# 2. a STAC Link Object with relation type "license"
LICENSE = {"title": "Non-commercial use only", "href": "https://www.geoportal.lt/metadata-catalog/catalog/search/resource/details.page?uuid=%7B7AF3F5B2-DC58-4EC5-916C-813E994B2DCF%7D", "type": "text/html", "rel": "license"}

# Map original column names to fiboa property names
# You also need to list any column that you may have added in the MIGRATION function (see below).
COLUMNS = {
    'NMA_ID' : 'id', # fiboa core field
    'GRUPE' : 'crop_type_name',#fiboa custom field
    'Shape_Leng' : 'perimeter', # fiboa core field
    'Shape_Area' : 'area',# fiboa core field
    'EC_trans_n' : 'ec:translated_name',
    'EC_hcat_n' : 'ec:hcat_name',
    'EC_hcat_c' : 'ec:hcat_code',
    'geometry' : 'geometry'# fiboa core field
}

# Add columns with constant values.
# The key is the column name, the value is a constant value that's used for all rows.
ADD_COLUMNS = {
    "determination_datetime": "2021-10-08T00:00:00Z"
}

# A list of implemented extension identifiers
EXTENSIONS = [
    "https://fiboa.github.io/hcat-extension/v0.1.0/schema.yaml"
]

# Functions to migrate data in columns to match the fiboa specification.
# Example: You have a column area_m in square meters and want to convert
# to hectares as required for the area field in fiboa.
# Function signature:
#   func(column: pd.Series) -> pd.Series

COLUMN_MIGRATIONS = {
    'Shape_Area': lambda column: column * 0.0001,
    #convert to int32
    'EC_hcat_c': lambda column: column.astype('uint32')
}

# Filter columns to only include the ones that are relevant for the collection,
# e.g. only rows that contain the word "agriculture" but not "forest" in the column "land_cover_type".
# Lamda function accepts a Pandas Series and returns a Series or a Tuple with a Series and True to inverse the mask.
COLUMN_FILTERS = {
    "GRUPE": lambda col: (col.isin(['Darþovës', 'Grikiai', 'Ankðtiniai javai', 'Aviþos', 'Þieminiai javai', 'Summer Cereals', 'Vasariniai javai','Cukriniai runkeliai', 'Uogynai', 'Kukurûzai']), False)
}


# There seem to be z-values in the geometry column, so using this to drop them.
def drop_z(geometry):
    if geometry.has_z:
        # Convert to 2D by dropping the Z dimension
        return shape(geometry).xy
    return geometry

def migrate(gdf):
    gdf['geometry'] = gdf['geometry'].apply(drop_z)
    return gdf

MIGRATION = migrate

# Custom function to execute actions on the the GeoDataFrame that are loaded from individual file.
# This is useful if the data is split into multiple files and columns should be added or changed
# on a per-file basis for example.
# The path contains the local path to the file that was read.
# The uri contains the URL that was read.
# Function signature:
#   func(gdf: gpd.GeoDataFrame, path: string, uri: string) -> gpd.GeoDataFrame
FILE_MIGRATION = None


# Schemas for the fields that are not defined in fiboa
# Keys must be the values from the COLUMNS dict, not the keys
MISSING_SCHEMAS = {
    # "required": ["my_id"], # i.e. non-nullable properties
    "properties": {
        "crop_type_name": {
            "type": "string"
        }
    }
}


# Conversion function, usually no changes required
def convert(output_file,  input_files = None, cache = None, source_coop_url = None, collection = False, compression = None):
    """
    Converts the field boundary datasets to fiboa.

    For reference, this is the order in which the conversion steps are applied:
    0. Read GeoDataFrame from file
    1. Run global migration (if provided through MIGRATION)
    2. Run filters to remove rows that shall not be in the final data
       (if provided through COLUMN_FILTERS)
    3. Add columns with constant values
    4. Run column migrations (if provided through COLUMN_MIGRATIONS)
    5. Duplicate columns (if an array is provided as the value in COLUMNS)
    6. Rename columns (as provided in COLUMNS)
    7. Remove columns (if column is not present as value in COLUMNS)
    8. Create the collection
    9. Change data types of the columns based on the provided schemas
    (fiboa spec, extensions, and MISSING_SCHEMAS)
    10. Write the data to the Parquet file

    Parameters:
    output_file (str): Path where the Parquet file shall be stored.
    cache): Path to a cached file of the data. Default: None.
                      Can be used to avoid repetitive downloads from the original data source.
    source_coop_url (str): URL to the (future) Source Cooperative repository. Default: None
    collection (bool): Additionally, store the collection separate from Parquet file. Default: False
    compression (str): Compression method for the Parquet file. Default: zstd
    kwargs: Additional keyword arguments for GeoPanda's read_file() or read_parquet() function.
    """
    convert_(
        output_file,
        cache,
        SOURCES,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        provider_name=PROVIDER_NAME,
        provider_url=PROVIDER_URL,
        source_coop_url=source_coop_url,
        extensions=EXTENSIONS,
        missing_schemas=MISSING_SCHEMAS,
        column_additions=ADD_COLUMNS,
        column_migrations=COLUMN_MIGRATIONS,
        column_filters=COLUMN_FILTERS,
        migrations=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )