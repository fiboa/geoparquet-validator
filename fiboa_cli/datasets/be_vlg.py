from ..convert_utils import convert as convert_

URI = "https://www.landbouwvlaanderen.be/bestanden/gis/Landbouwgebruikspercelen_2023_-_Definitief_(extractie_28-03-2024)_GPKG.zip"
# in the zipfile, the gpkg has a different name so we need to add this while opening the file
PATH_TEMPLATE = "/vsizip/{}/Landbouwgebruikspercelen_2023_-_Definitief_(extractie_28-03-2024).gpkg"

ID = "be_vlg"
TITLE = "Crop field boundaries for Belgium - Flanders"
DESCRIPTION = """
Since 2020, the Department of Agriculture and Fisheries has been publishing a more extensive set of data related to agricultural use plots (from the 2008 campaign).

From 2023, the downloadable dataset of agricultural use plots will also include the specialization given by the company (= company typology) and that is given to the plots of the company. Based on the typology, the companies are divided into 4 major specializations: arable farming, horticulture, livestock farming and mixed farms. The specialization of each company is calculated annually according to a European method and is based on the standard output of the various agricultural productions on the company. It is therefore an economic specialization and not a reflection of all agricultural production on the company.
"""
BBOX = [2.531029700947351, 50.67422857041101, 5.932736829503416, 51.49545715218195]

PROVIDER_NAME = "Agentschap Landbouw & Zeevisserij (Government)"
PROVIDER_URL = "https://landbouwcijfers.vlaanderen.be/open-geodata-landbouwgebruikspercelen"

ATTRIBUTION = ""
LICENSE = "CC0"  # Publiek / Toegang zonder voorwaarden

# TODO we skip many, specific columns. Check the data survey or included documentation for more available columns
COLUMNS = {
    "BT_BRON": "source",
    "BT_OMSCH": "typology",
    "GRAF_OPP": "area",
    "REF_ID": "id",
    "GWSCOD_H": "crop_code",
    "GWSNAM_H": "crop_name",
}

ADD_COLUMNS = {
    "determination_datetime": "2024-03-28T00:00:00Z"
}

EXTENSIONS = []
COLUMN_MIGRATIONS = {}
COLUMN_FILTERS = {}

MIGRATION = None

MISSING_SCHEMAS = {
    "properties": {
        "source": {
            "type": "string"
        },
        "crop_code": {
            "type": "string"
        },
        "crop_name": {
            "type": "string"
        },
        "typology": {
            "type": "string"
        }
    }
}


# Conversion function, usually no changes required
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
        path_template=PATH_TEMPLATE
    )
