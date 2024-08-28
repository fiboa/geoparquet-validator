from ..convert_utils import convert as convert_
from .commons.ec import add_eurocrops
from . import fr as base

SOURCES = {
    "https://zenodo.org/records/10118572/files/FR_2018.zip": [
        "FR_2018/FR_2018_EC21.shp"
    ]
}

ID, SHORT_NAME, TITLE, DESCRIPTION, PROVIDERS, EXTENSIONS, COLUMNS, LICENSE = add_eurocrops(base, 2018)

def convert(output_file, cache = None, **kwargs):
    convert_(
        output_file,
        cache,
        SOURCES,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        extensions=EXTENSIONS,
        providers=PROVIDERS,
        missing_schemas=base.MISSING_SCHEMAS,
        column_filters=base.COLUMN_FILTERS,
        attribution=base.ATTRIBUTION,
        license=LICENSE,
        explode_multipolygon=True,
        **kwargs
    )
