from ..convert_utils import convert as convert_
import pandas as pd

URI = "http://use_your_own_copy/"
# Call `fiboa convert be_wa -c your_own_downloaded_file` . See data-survey for data acquisition instructions

ID = "be_wa"
TITLE = "Belgium Wallonia: Parcellaire Agricole Anonyme"
DESCRIPTION = """
The Crop Fields (PAA) covers land use in agricultural and forestry areas managed as part of the implementation of the Common Agricultural Policy by the Paying Agency of Wallonia.

The PAA represents the public version of the agricultural plot. It therefore does not include personal information allowing the operator to be identified. It is provided on an annual basis. Data from a year of cultivation are made available to the public during the following year.

You can download the data yourself, but the license does not allow public distribution. You can obtain a personal/company license for free, or freely use a WMS service for visualization.
"""
BBOX = [2.840195234589047, 49.48469172050167, 6.4282309142607055, 50.80877425797556]

PROVIDER_NAME = "Service public de Wallonie (SPW)"
PROVIDER_URL = "https://geoportail.wallonie.be/catalogue/49294570-2a8d-49ca-995c-1b0890672bc8.html"
ATTRIBUTION = "Service public de Wallonie (SPW)"

LICENSE = {
    "title": "Conditions générales d’utilisation des données géographiques numériques du Service public de Wallonie",
    "href": "https://geoportail.wallonie.be/files/documents/ConditionsSPW/DataSPW-CGU.pdf",
    "type": "text/html",
    "rel": "license"
}

COLUMNS = {
    "geometry": "geometry",
    "OBJECTID": "id",
    "SURF_HA": "area",
    "CAMPAGNE": "determination_datetime",
    'CULT_COD': 'crop_code',
    'CULT_NOM': 'crop_name',
    'GROUPE_CULT': 'group_code',
}

ADD_COLUMNS = {}

# A list of implemented extension identifiers
EXTENSIONS = []

COLUMN_MIGRATIONS = {
    "determination_datetime": lambda col: pd.to_datetime(col, format='%Y') + pd.DateOffset(months=4, days=14)
}

COLUMN_FILTERS = {}

MIGRATION = None

MISSING_SCHEMAS = {
    "properties": {
        "crop_code": {
            "type": "string"
        },
        "crop_name": {
            "type": "string"
        },
        "group_code": {
            "type": "string"
        },
    }
}

def convert(output_file, cache_file = None, source_coop_url = None, collection = False, compression = None):
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
        license=LICENSE,
        compression=compression,
    )
