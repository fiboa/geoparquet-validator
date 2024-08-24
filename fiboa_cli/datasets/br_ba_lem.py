from ..convert_utils import convert as convert_

SOURCES = {
    "https://data.mendeley.com/public-files/datasets/vz6d7tw87f/files/57c83c3f-b5a9-45f5-94f8-ac1df8fab923/file_downloaded": [
      "LEM_dataset.shp"
    ]
}

ID = "br_ba_lem"
SHORT_NAME = "West Bahia, Brazil"
TITLE = "Field boundaries for the west of Bahia state, Brazil"
DESCRIPTION = """
This dataset is the supplementary data of a paper published in the Data in Brief Journal.

The dataset, in ESRI shapefile format (spatial reference system: WGS 84, EPSG: 4326), provides monthly land use
information about 1854 fields from October 2019 to September 2020 from Luís Eduardo Magalhães (LEM) and other
municipalities in the west of Bahia state, Brazil. The majority of the 16 land uses classes are related to crops.
"""

PROVIDERS = [
    {
        "name": "Mendeley Data",
        "url": "https://data.mendeley.com/datasets/vz6d7tw87f/1#file-5ac1542b-12ef-4dce-8258-113b5c5d87c9",
        "roles": ["producer", "licensor"]
    }
]

ATTRIBUTION = "Copyright © 2024 Elsevier inc, its licensors, and contributors."
LICENSE = "CC-BY-4.0"

COLUMNS = {
    'geometry': 'geometry',
    'id': 'id',
    'Oct_2019': '2019-10',
    'Nov_2019': '2019-11',
    'Dec_2019': '2019-12',
    'Jan_2020': '2020-01',
    'Feb_2020': '2020-02',
    'Mar_2020': '2020-03',
    'Apr_2020': '2020-04',
    'May_2020': '2020-05',
    'Jun_2020': '2020-06',
    'Jul_2020': '2020-07',
    'Aug_2020': '2020-08',
    'Sep_2020': '2020-09',
    'Note': 'note'
}

TYPE_SCHEMA = {
    "type": "string",
    "enum": [
        "Beans",
        "Brachiaria",
        "Cerrado",
        "Coffee",
        "Conversion area",
        "Corn",
        "Cotton",
        "Crotalaria",
        "Eucalyptus",
        "Hay",
        "Millet",
        "Not identified",
        "Pasture",
        "Sorghum",
        "Soybean",
        "Uncultivated soil",
    ]
}
MISSING_SCHEMAS = {
    "required": ["2019-10", "2019-11", "2019-12", "2020-01", "2020-02", "2020-03", "2020-04", "2020-05", "2020-06", "2020-07", "2020-08", "2020-09"],
    "properties": {
        "2019-10": TYPE_SCHEMA,
        "2019-11": TYPE_SCHEMA,
        "2019-12": TYPE_SCHEMA,
        "2020-01": TYPE_SCHEMA,
        "2020-02": TYPE_SCHEMA,
        "2020-03": TYPE_SCHEMA,
        "2020-04": TYPE_SCHEMA,
        "2020-05": TYPE_SCHEMA,
        "2020-06": TYPE_SCHEMA,
        "2020-07": TYPE_SCHEMA,
        "2020-08": TYPE_SCHEMA,
        "2020-09": TYPE_SCHEMA,
        "note": {
            "type": "string"
        }
    }
}


# Conversion function, usually no changes required
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
        attribution=ATTRIBUTION,
        license=LICENSE,
        **kwargs
    )
