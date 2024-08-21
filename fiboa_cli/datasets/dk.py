from ..convert_utils import convert as convert_

SOURCES = "https://landbrugsgeodata.fvm.dk/Download/Marker/Marker_2023.zip"
LAYER_FILTER = None
ID = "dk"
SHORT_NAME = "Denmark"
TITLE = "Denmark Crop Fields (Marker)"
DESCRIPTION = """The Danish Ministry of Food, Agriculture and Fisheries publishes Crop Fields (Marker) for each year."""

PROVIDERS = [
    {
        "name": "Ministry of Food, Agriculture and Fisheries of Denmark",
        "url": "https://fvm.dk/",
        "roles": ["licensor"]
    },
    {
        "name": "Danish Agricultural Agency",
        "url": "https://lbst.dk/",
        "roles": ["producer", "licensor"]
    }
]

LICENSE = "CC-0"
COLUMNS = {
    'geometry': 'geometry',
    'Marknr': 'id',
    'IMK_areal': "area",
    'Afgkode': 'crop_code',
    'Afgroede': 'crop_name',
}

MISSING_SCHEMAS = {
    "properties": {
        "crop_name": {
            "type": "string"
        },
        "crop_code": {
            "type": "string"
        },
    }
}


def convert(output_file, input_files = None, cache = None, source_coop_url = None, collection = False, compression = None):
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
        missing_schemas=MISSING_SCHEMAS,
        layer_filter=LAYER_FILTER,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )
