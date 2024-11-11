from ..convert_utils import convert as convert_
from .commons.ec import add_eurocrops
from . import nl_crop as base


SOURCES = {
    "https://zenodo.org/records/10118572/files/NL_2020.zip?download=1": [
        "NL_2020_EC21.shp"
    ]
}

ID, SHORT_NAME, TITLE, DESCRIPTION, PROVIDERS, EXTENSIONS, COLUMNS, LICENSE = add_eurocrops(base, 2020)

COLUMNS = COLUMNS | {
    "gewascateg": COLUMNS["category"],
    "length": "perimeter",
    "area": "area",
    "objectid": "id"
}
del COLUMNS["category"], COLUMNS["id"], COLUMNS["jaar"]

COLUMN_MIGRATIONS = {
    "area": lambda col: col / 10000,
}

COLUMN_FILTERS = base.COLUMN_FILTERS | {
    "gewascateg": base.COLUMN_FILTERS["category"]
}
del COLUMN_FILTERS["category"]

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
        extensions=EXTENSIONS,
        missing_schemas=base.MISSING_SCHEMAS,
        column_migrations=COLUMN_MIGRATIONS,
        column_filters=COLUMN_FILTERS,
        attribution=base.ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )
