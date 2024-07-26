from ..convert_utils import convert as convert_

# Please REMOVE the comment after review: 
# I was actually using this link "https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/vz6d7tw87f-1.zip" which is copied from the 
# "Download All" button from the website (https://data.mendeley.com/datasets/vz6d7tw87f/1). 
# But output an error saying """/vsizip//tmp/tmp7puy6g90/vz6d7tw87f-1.zip' not recognized as a supported file format. 
# It might help to specify the correct driver explicitly by prefixing the file path with '<DRIVER>:', e.g. 'CSV:path'""""
SOURCES = {
    "https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/vz6d7tw87f-1.zip": "LEM_dataset.shp"
}


ID = "br"
SHORT_NAME = "Brazil"
TITLE = "Field boundaries for Brazil"
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
    'OCT_2019': '2019-10',
    'NOV_2019': '2019-11',
    'DEC_2019': '2019-12',
    'JAN_2020': '2020-01',
    'FEB_2020': '2020-02',
    'MAR_2020': '2020-03',
    'APR_2020': '2020-04',
    'MAY_2020': '2020-05',
    'JUN_2020': '2020-06',
    'JUL_2020': '2020-07',
    'AUG_2020': '2020-08',
    'SEP_2020': '2020-09',
    'NOTE': 'note'
}

ADD_COLUMNS = {
}

EXTENSIONS = []

COLUMN_MIGRATIONS = {
}

COLUMN_FILTERS = {
}

MIGRATION = None

FILE_MIGRATION = None

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
    },
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
        missing_schemas=MISSING_SCHEMAS,
        column_additions=ADD_COLUMNS,
        column_migrations=COLUMN_MIGRATIONS,
        column_filters=COLUMN_FILTERS,
        migration=MIGRATION,
        file_migration=FILE_MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )
