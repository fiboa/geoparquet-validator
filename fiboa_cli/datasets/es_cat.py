from ..convert_utils import convert as convert_
import pandas as pd

SOURCES = {
    "https://analisi.transparenciacatalunya.cat/api/views/yh94-j2n9/files/b4299961-52ee-4fa0-a276-4594c8c094bc?download=true&filename=Cultius_DUN2023_GPKG.zip": ["Cultius_DUN2023_GPKG/CULTIUS_DUN2023.gpkg"]
}
ID = "es_cat"
SHORT_NAME = "Catalonia"
TITLE = "Catalonia Crop Fields (Mapa de cultius)"
DESCRIPTION = """
The Department of Agriculture, Livestock, Fisheries and Food makes available to the public the data from the crop map of Catalonia.
This map allows you to locate the crops declared in the Agrarian Declaration - DUN submitted to the DACC.
"""
PROVIDERS = [
    {
        "name": "Catalonia Department of Agriculture, Livestock, Fisheries and Food",
        "url": "https://agricultura.gencat.cat/ca/ambits/desenvolupament-rural/sigpac/mapa-cultius/",
        "roles": ["producer", "licensor"]
    }
]
ATTRIBUTION = "Catalonia Department of Agriculture, Livestock, Fisheries and Food"
LICENSE = {
    "title": "The Open Information Use License - Catalonia",
    "href": "https://administraciodigital.gencat.cat/ca/dades/dades-obertes/informacio-practica/llicencies/",
    "type": "text/html",
    "rel": "license"
}
COLUMNS = {
    "geometry": "geometry",
    "id": "id",
    "campanya": "determination_datetime",
    "ha": "area",
    "cultiu": "crop_name",
}

COLUMN_MIGRATIONS = {
    "campanya": lambda col: pd.to_datetime(col, format='%Y'),
    "geometry": lambda col: col.make_valid(),
}

MISSING_SCHEMAS = {
    "properties": {
        "crop_name": {
            "type": "string"
        },
    }
}

def convert(output_file, cache = None, **kwargs):
    convert_(
        output_file,
        cache,
        SOURCES,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        providers=PROVIDERS,
        missing_schemas=MISSING_SCHEMAS,
        column_migrations=COLUMN_MIGRATIONS,
        attribution=ATTRIBUTION,
        license=LICENSE,
        layer="CULTIUS_DUN2023",
        index_as_id=True,
        explode_multipolygon=True,
        **kwargs
    )
