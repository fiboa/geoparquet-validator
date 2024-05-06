# TEMPLATE FOR A FIBOA CONVERTER
#
# Copy this file and rename it to something sensible.
# The name of the file will be the name of the converter in the cli.
# If you name it 'de_abc' you'll be able to run `fiboa convert de_abc` in the cli.

from ..convert_utils import convert as convert_

# File to read the data from
# Can read any tabular data format that GeoPandas can read through read_file()
# Supported protcols: HTTP(S), GCS, S3, or the local file system
URI = "https://fiboa.example/file.xyz"

# Unique identifier for the collection
ID = "abc"
# Title of the collection
TITLE = "Field boundaries for ABC"
# Description of the collection. Can be multiline and include CommonMark.
DESCRIPTION = """Describe the dataset here."""
# Bounding box of the data in WGS84 coordinates
BBOX = [7.8685145620,53.3590675115,11.3132037822,55.0573747014]

# Provider name, can be None if not applicable, must be provided if PROVIDER_URL is provided
PROVIDER_NAME = "ABC Corp."
# URL to the homepage of the data or the provider, can be None if not applicable
PROVIDER_URL = "https://abc.example"
# Attribution, can be None if not applicable
ATTRIBUTION = "© 2024 ABC Corp."

# License of the data, either
# 1. a SPDX license identifier (including "dl-de/by-2-0" / "dl-de/zero-2-0"), or
LICENSE = "CC-BY-4.0"
# 2. a STAC Link Object with relation type "license"
# LICENSE = {"title": "CC-BY-4.0", "href": "https://creativecommons.org/licenses/by/4.0/", "type": "text/html", "rel": "license"}

# Map original column names to fiboa property names
# You also need to list any column that you may have added in the MIGRATION function (see below).
COLUMNS = {
    "area_m": "area"
}

# Add columns with constant values.
# The key is the column name, the value is a constant value that's used for all rows.
ADD_COLUMNS = {
    "determination_datetime": "2020-01-01T00:00:00Z"
}

# A list of implemented extension identifiers
EXTENSIONS = []

# Functions to migrate data in columns to match the fiboa specification.
# Example: You have a column area_m in square meters and want to convert
# to hectares as required for the area field in fiboa.
# Function signature:
#   func(column: pd.Series) -> pd.Series
COLUMN_MIGRATIONS = {
    "area_m": lambda column: column * 0.0001
}

# Filter columns to only include the ones that are relevant for the collection,
# e.g. only rows that contain the word "agriculture" but not "forest" in the column "land_cover_type".
# Lamda function accepts a Pandas Series and returns a Series or a Tuple with a Series and True to inverse the mask.
COLUMN_FILTERS = {
    "land_cover_type": lambda col: (col.isin(["agrictulture"]), True)
}

# Custom function to migrate the GeoDataFrame if the other options are not sufficient
# This should be the last resort!
# Function signature:
#   func(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame
MIGRATION = None

# Schemas for the fields that are not defined in fiboa
# Keys must be the values from the COLUMNS dict, not the keys
MISSING_SCHEMAS = {
    "required": ["my_id"], # i.e. non-nullable properties
    "properties": {
        "my_id": {
            "type": "string"
        }
    }
}


# Conversion function, usually no changes required
def convert(output_file, cache_file = None, source_coop_url = None, collection = False):
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
        column_additions=ADD_COLUMNS,
        column_migrations=COLUMN_MIGRATIONS,
        column_filters=COLUMN_FILTERS,
        migration=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE
    )
