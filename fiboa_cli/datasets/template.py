from ..convert_utils import convert as convert_

# File to read the data from, can be HTTP(S), GCS, S3, or local file
URI = "https://fiboa.example/file.xyz"
# Unique identifier for the collection
ID = "abc"
# Title of the collection
TITLE = "Field boundaries for ABC"
# Description of the collection. Can be multiline and include CommonMark.
DESCRIPTION = """Describe the dataset here."""
# Attribution, can be None if not applicable
ATTRIBUTION = "© 2024 ABC Corp."
# Provider name, can be None if not applicable, must be provided if PROVIDER_URL is provided
PROVIDER_NAME = "ABC Corp."
# URL to the homepage of the data or the provider, can be None if not applicable
PROVIDER_URL = "https://abc.example"
# SPDX license identifier or a STAC Link Object with relation type "license"
LICENSE = "CC-BY-4.0"
# Bounding box of the data in WGS84 coordinates
BBOX = [7.8685145620,53.3590675115,11.3132037822,55.0573747014]
# A list of implemented extension identifiers
EXTENSIONS = []
# Map original column names to fiboa property names
COLUMNS = {
    'old': 'new'
}
# Schemas for the fields that are not defined in fiboa
# Keys must be the values from the COLUMNS dict, not the keys
MISSING_SCHEMAS = {
    'required': ['new'], # i.e. non-nullable properties
    'properties': {
        'new': {
            'type': 'string'
        }
    }
}

# Conversion function, usually no changes required
def convert(output_file, cache_file = None, source_coop_url = None, collection = False):
    """
    Converts the field boundary datasets to fiboa.
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
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE
    )
