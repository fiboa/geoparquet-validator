# Digifarm converter for fiboa
# First draft just takes in a file saved from the API.
# It'd be ideal if there was a CLI that took a Digifarm token and BBOX and would save the file locally and
# convert it. Not sure if we want to keep loading functionality into the converter, if we have a new
# CLI tool that could query field boundary API's - I'd see DigiFarm and Onesoil as options, where you could
# do like 'fiboa api-request digifarm --bbox 4,12,5,13 --token blah | fiboa convert digifarm -i -'.

from ..convert_utils import convert as convert_

# File(s) to read the data from. This should be a GeoJSON file saved from the Digifarm API. 
#
# I'm including a 'source' commented out for testing, but setting source to 'None' so
# that the user knows they need to get the data from the API.
# SOURCES = "https://gist.githubusercontent.com/cholmes/de82be680b6f9a14ea9e61cf5cbd9c91/raw/2b30dd30a6ed409c23f5181272e20ef3acd4808e/digifarm-norway-sample.json"
SOURCES = None

DATA_ACCESS = """
Data must be obtained from the Digifarm API, see https://api-docs.digifarm.io/. Just save any
API output to a GeoJSON file (with .json as the file extension) and use that as the input file.
If you want some DigiFarm data, anyone can use:
https://api.digifarm.io/v1/delineated-fields?token=a0731a8c-5259-4c68-af3a-7ad4f6d53faa&bbox=11.13,60.72,11.21,60.76
Just save the result as something like 'digifarm-sample.json' and then use -i with the file name to convert it.
"""

# A filter function for the layer in the file(s) to read.
# Set to None if the file contains only one layer or all layers should be read.
# Function signature:
#   func(layer: str, path: str) -> bool
LAYER_FILTER = None

# Unique identifier for the collection
ID = "digifarm"
# Geonames for the data (e.g. Country, Region, Source, Year)
SHORT_NAME = "DigiFarm Field Boundaries"
# Title of the collection
TITLE = "Field boundaries created by DigiFarm Automatic Field Delineation Model"
# Description of the collection. Can be multiline and include CommonMark.
DESCRIPTION = """These field boundaries are created by DigiFarm, using an  state-of-the-art deep neural network model for Field Delineation
from super-resolved satellite imagery. The results are available through an API, covering over 200 million hectares across 30+ countries. 
The data is provided through the DigiFarm API at https://api-docs.digifarm.io/, as GeoJSON. For more information see https://digifarm.io/products/field-boundaries
"""

# A list of providers that contributed to the data.
# This should be an array of Provider Objects:
# https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#provider-object
PROVIDERS = [
    {
        "name": "DigiFarm",
        "url": "https://digifarm.io",
        "roles": ["producer", "licensor"]
    }
]

# Attribution (e.g. copyright or citation statement as requested by provider).
# The attribution is usually shown on the map, in the lower right corner.
# Can be None if not applicable
ATTRIBUTION = "© 2024 digifarm.io"

# License of the data, either
# 1. a SPDX license identifier (including "dl-de/by-2-0" / "dl-de/zero-2-0"), or
# LICENSE = "CC-BY-4.0"
# 2. a STAC Link Object with relation type "license"
LICENSE = {"title": "Proprietary", "href": "https://digifarm.io/legal/tc", "type": "text/html", "rel": "license"}

# Map original column names to fiboa property names
# You also need to list any column that you may have added in the MIGRATION function (see below).
COLUMNS = {
    # This is using shapefile input, and seems to get capitalized somehow, but geopandas didn't 
    # recognize it as 'area'. Wondering if we could make it case insensitive?
    "AREA": "area",
    "id": "id",
    "geometry": "geometry",
    "area_ha": "area_sq_m",
    "area_acres": "area_acres"
}

# Add columns with constant values.
# The key is the column name, the value is a constant value that's used for all rows.
ADD_COLUMNS = {
  "determination_method": "auto-imagery"
}

# A list of implemented extension identifiers
EXTENSIONS = []

COLUMN_MIGRATIONS = {
    # This seems a bit goofy to swap the columns, but 'area' is always present in digifarm
    # responses, while area_ha is sometimes not present - particularly in bulk files.
    "AREA": lambda column: column * 0.0001,
    "area_ha": lambda column: column * 10000
}

# Schemas for the fields that are not defined in fiboa
# Keys must be the values from the COLUMNS dict, not the keys
MISSING_SCHEMAS = {
    "properties": {
        "area_sq_m": {
            "type": "float"
        },
        "area_acres": {
            "type": "float"
        }   
    }
}


# Conversion function, usually no changes required
def convert(output_file, input_files = None, cache = None, source_coop_url = None, collection = False, compression = None):
    """
    Converts the field boundary datasets to fiboa.

    For reference, this is the order in which the conversion steps are applied:
    0. Read GeoDataFrame from file(s) / layer(s) and run the FILE_MIGRATION function if provided
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
        input_files=input_files,
        providers=PROVIDERS,
        source_coop_url=source_coop_url,
        extensions=EXTENSIONS,
        column_migrations=COLUMN_MIGRATIONS,
        missing_schemas=MISSING_SCHEMAS,
        column_additions=ADD_COLUMNS,
        layer_filter=LAYER_FILTER,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )
