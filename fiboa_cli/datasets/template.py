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
# 1. a SPDX license identifier, or
LICENSE = "CC-BY-4.0"
# 2. a STAC Link Object with relation type "license"
# LICENSE = {'title': 'CC-BY-4.0', 'href': 'https://creativecommons.org/licenses/by/4.0/', 'type': 'text/html', 'rel': 'license'}

# A list of implemented extension identifiers
EXTENSIONS = []

# Map original column names to fiboa property names
COLUMNS = {
    'area_m': 'area'
}

# Functions to migrate data in columns to match the fiboa specification.
# Example: You have a column area_m in square meters and want to convert
# to hectares as required for the area field in fiboa.
# Function signature:
#   func(column: pd.Series) -> pd.Series
COLUMN_MIGRATIONS = {
    'area_m': lambda column: column * 0.0001
}

# Custom function to migrate the GeoDataFrame if the other options are not sufficient
# This should be the last resort!
# Function signature:
#   func(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame
MIGRATION = None

# Schemas for the fields that are not defined in fiboa
# Keys must be the values from the COLUMNS dict, not the keys
MISSING_SCHEMAS = {
    'required': ['my_id'], # i.e. non-nullable properties
    'properties': {
        'my_id': {
            'type': 'string'
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
    2. Run column migrations (if provided through COLUMN_MIGRATIONS)
    3. Duplicate columns (if an array is provided as the value in COLUMNS)
    4. Rename columns (as provided in COLUMNS)
    5. Remove columns (if column is not present as value in COLUMNS)
    6. Create the collection
    7. Change data types of the columns based on the provided schemas
    (fiboa spec, extensions, and MISSING_SCHEMAS)
    8. Write the data to the Parquet file

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
        migration=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE
    )
