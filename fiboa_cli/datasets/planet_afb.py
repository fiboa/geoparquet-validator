# Converter for geopackage output of Planet's Field Boundaries dataset.

from ..convert_utils import convert as convert_

# This local path should not be the main way this converter work. When there's some open data
# we could point at that, but the ideal way this converter should work is by letting the user pass
# in a local or remote path that is produced by Planet's field boundary output, converting
# whatever data is in it into fiboa. Not sure how to do that yet, seems like a -i flag could work
# to complement -o, and then perhaps allow this converter to make it required? Or it could
# default to an open dataset, but then have -i override (and check to make sure it matches)
SOURCES = "/Users/cholmes/repos/data-fiboa/source/FIELD_BOUNDARIES_v1.0.0_S2_P1M-20230101T000000Z_fb.gpkg"

# Unique identifier for the collection
ID = "planet_afb"
# Geonames for the data (e.g. Country, Region, Source, Year)
SHORT_NAME = "Planet Field Boundaries"
# Title of the collection
TITLE = "Field boundaries created by Planet's Automated Field Boundary detection algorithm"
# Description of the collection. Can be multiline and include CommonMark.
DESCRIPTION = "These field boundaries were originally created by Planet's automated field boundary detection algorithm, and converted to the fiboa format."

# Provider name, can be None if not applicable, must be provided if PROVIDER_URL is provided
PROVIDER_NAME = "Planet Labs, PBC"
# URL to the homepage of the data or the provider, can be None if not applicable
PROVIDER_URL = "https://planet.com"
# Attribution, can be None if not applicable
ATTRIBUTION = "© 2024 Planet Labs, PBC"

# License of the data, either
# 1. a SPDX license identifier (including "dl-de/by-2-0" / "dl-de/zero-2-0"), or
# LICENSE = "CC-BY-4.0"
# 2. a STAC Link Object with relation type "license"
LICENSE = {"title": "Proprietary License", "href": "https://www.planet.com/licensing-information/", "type": "text/html", "rel": "license"}

# Map original column names to fiboa property names
# You also need to list any column that you may have added in the MIGRATION function (see below).
COLUMNS = {
    "polygon_id": "id", #fiboa core field
    "area_ha": "area", #fiboa core field
    "geometry": "geometry", #fiboa core field
    "ca_ratio": "ca_ratio", #custom field for Planet
    "micd": "micd", #custom field for Planet
    "qa": "qa", #custom field for Planet
}

# Add columns with constant values.
# The key is the column name, the value is a constant value that's used for all rows.
ADD_COLUMNS = {
    # Hardcoding for this particular dataset, but Planet currently does not have a date column,
    # but does include the date in the filename. So not sure if there's a way to pull the date
    # from the filename? I suppose not now when there's no way to pass in the file name...
    # "determination_datetime": "2023-01-01T00:00:00Z"
}

# A list of implemented extension identifiers
EXTENSIONS = []

# Functions to migrate data in columns to match the fiboa specification.
# Example: You have a column area_m in square meters and want to convert
# to hectares as required for the area field in fiboa.
# Function signature:
#   func(column: pd.Series) -> pd.Series
COLUMN_MIGRATIONS = {
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
    "required": ["ca_ratio", "micd", "qa" ], # i.e. non-nullable properties
    "properties": {
        "ca_ratio": {
            "type": "float"
        },
        "micd": {
            "type": "float"
        },
        "qa": {
            "type": "uint8"
        }
    }
}


# Conversion function, usually no changes required
def convert(output_file, cache = None, source_coop_url = None, collection = False, compression = None):
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
    cache (str): Path to a cached folder for the data. Default: None.
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
        migration=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )
