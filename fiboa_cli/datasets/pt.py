from ..convert_utils import convert as convert_
import geopandas as gpd
import pandas as pd
import fiona

SOURCES = "https://www.ifap.pt/isip/ows/resources/2023/Continente.gpkg"
#SOURCES = "https://www.ifap.pt/isip/ows/resources/2022/2022.zip"

ID = "pt"
TITLE = "Field boundaries for Portugal (identificação de parcelas)"
DESCRIPTION = """Open field boundaries from Portugal"""
PROVIDER_NAME = "IPAP - Instituto de Financiamento da Agricultura e Pescas"
PROVIDER_URL = "https://www.ifap.pt/isip/ows/"
ATTRIBUTION = None

# Inspire license. Not 100% clear at source
LICENSE = {"title": "No conditions apply", "href": "https://inspire.ec.europa.eu/metadata-codelist/ConditionsApplyingToAccessAndUse/noConditionsApply", "type": "text/html", "rel": "license"}

COLUMNS = {
    "geometry": "geometry",
    "OSA_ID": "id",
    "CUL_ID": "block_id",
    "CUL_CODIGO": "crop_code",
    "CT_português": "crop_name",
    "Shape_Area": "area"
}

ADD_COLUMNS = {
    "determination_datetime": "2023-01-01T00:00:00Z"
}

COLUMN_MIGRATIONS = {
    "Shape_Area": lambda col: col / 10000.0
}

MISSING_SCHEMAS = {
    "properties": {
        "block_id": {
            "type": "int64"
        },
        "crop_code": {
            "type": "string"
        },
        "crop_name": {
            "type": "string"
        },
    }
}


def file_migration(data, path, uri):
    # filter for layers starting with Culturas_
    gdfs = []
    layers = fiona.listlayers(path)
    for layer in layers:
        if not layer.startswith("Culturas_"):
            continue
        data = gpd.read_file(path, layer=layer)
        gdfs.append(data)

    return pd.concat(gdfs)


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
        provider_name=PROVIDER_NAME,
        provider_url=PROVIDER_URL,
        source_coop_url=source_coop_url,
        missing_schemas=MISSING_SCHEMAS,
        column_migrations=COLUMN_MIGRATIONS,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
        file_migration=file_migration
    )
