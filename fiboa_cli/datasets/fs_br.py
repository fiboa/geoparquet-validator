from ..convert_utils import convert as convert_
import re
import pandas as pd

# Please REMOVE the comment after review: 
# I was actually using this link "https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/vz6d7tw87f-1.zip" which is copied from the 
# "Download All" button from the website (https://data.mendeley.com/datasets/vz6d7tw87f/1). 
# But output an error saying """/vsizip//tmp/tmp7puy6g90/vz6d7tw87f-1.zip' not recognized as a supported file format. 
# It might help to specify the correct driver explicitly by prefixing the file path with '<DRIVER>:', e.g. 'CSV:path'""""
SOURCES = "https://github.com/fiboa/data/files/15438544/LEM_dataset.zip"


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

# Please REMOVE the comment after review: 
# The commented variables are my proposed columns attributes, but it doesn't seem 
# logical to me to have months and years as the data's attributes. What do you think?
COLUMNS = {
    'geometry': 'geometry',
    'id': 'id',
    # 'OCT_2019': 'oct_2019',
    # 'NOV_2019': 'nov_2019',
    # 'DEC_2019': 'dec_2019',
    # 'JAN_2020': 'jan_2020',
    # 'FEB_2020': 'feb_2020',
    # 'MAR_2020': 'mar_2020',
    # 'APR_2020': 'apr_2020',
    # 'MAY_2020': 'may_2020',
    # 'JUN_2020': 'jun_2020',
    # 'JUL_2020': 'jul_2020',
    # 'AUG_2020': 'aug_2020',
    # 'SEP_2020': 'sep_2020',
    # 'NOTE': 'note'
}

# Please REMOVE the comment after review: 
# I do think it is safe to assume the determination date is the last date of September in 2020 as the dataset
# has SEP_2020 (which is the latests time) as one of its column attributes.
ADD_COLUMNS = {
    "determination_datetime": "2020-09-30T00:00:00Z"
}

EXTENSIONS = []

COLUMN_MIGRATIONS = {
}

COLUMN_FILTERS = {
}

MIGRATION = None

FILE_MIGRATION = None

# Please REMOVE the comment after review: 
# There is one field that I omitted, which is the `NOTE` field in the dataset.
# This is because it is nullable.
MISSING_SCHEMAS = {
    # "required": ["oct_2019"],
    # "properties": {
    #     "oct_2019": {
    #         "type": "string"
    #     }
    # },
    # "required": ["nov_2019"], 
    # "properties": {
    #     "nov_2019": {
    #         "type": "string"
    #     }
    # },
    # "required": ["dec_2019"], 
    # "properties": {
    #     "dec_2019": {
    #         "type": "string"
    #     }
    # },
    # "required": ["jan_2020"],
    # "properties": {
    #     "jan_2020": {
    #         "type": "string"
    #     }
    # },
    # "required": ["feb_2020"],
    # "properties": {
    #     "feb_2020": {
    #         "type": "string"
    #     }
    # },
    # "required": ["mar_2020"],
    # "properties": {
    #     "mar_2020": {
    #         "type": "string"
    #     }
    # },
    # "required": ["apr_2020"],
    # "properties": {
    #     "apr_2020": {
    #         "type": "string"
    #     }
    # },
    # "required": ["may_2020"],
    # "properties": {
    #     "may_2020": {
    #         "type": "string"
    #     }
    # },
    # "required": ["jun_2020"],
    # "properties": {
    #     "jun_2020": {
    #         "type": "string"
    #     }
    # },
    # "required": ["jul_2020"],
    # "properties": {
    #     "jun_2020": {
    #         "type": "string"
    #     }
    # },
    # "required": ["aug_2020"],
    # "properties": {
    #     "aug_2020": {
    #         "type": "string"
    #     }
    # },
    # "required": ["sep_2020"],
    # "properties": {
    #     "sep_2020": {
    #         "type": "string"
    #     }
    # },
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
