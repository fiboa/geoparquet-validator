from ..convert_utils import convert as convert_

SOURCES = "https://www.apprrr.hr/wp-content/uploads/nipp/land_parcels.gpkg"

LAYER_FILTER = None


ID = "hr"
SHORT_NAME = "Republic of Croatia"
TITLE = "Croatian Agricultural Spatial Data"
DESCRIPTION = """
This dataset contains spatial data related to agricultural land use in Croatia, including ARKOD parcel information, 
environmentally sensitive areas, High Nature Value Grasslands, protective buffer strips around watercourses, and vineyard 
classifications. The data is crucial for managing agricultural activities, ensuring compliance with environmental regulations, 
and supporting sustainable land use practices.
"""

PROVIDERS = [
    {
        "name": "Agencija za plaćanja u poljoprivredi, ribarstvu i ruralnom razvoju",
        "url": "https://www.apprrr.hr/prostorni-podaci-servisi/",
        "roles": ["producer", "licensor"]
    }
]

ATTRIBUTION = "copyright © 2024. Agencija za plaćanja u poljoprivredi, ribarstvu i ruralnom razvoju"

#TODO: I could not find information about the license. 
LICENSE = ""


# {'properties': {'land_use_id': 'float', 'home_name': 'str:60', 'area': 'float', 'perim': 'float',
#                 'slope': 'float', 'z_avg': 'float', 'eligibility_coef': 'float', 'mines_status': 'str:1',
#                 'mines_year_removed': 'int', 'water_protect_zone': 'str:10', 'natura2000': 'float', 
#                 'natura2000_ok': 'str:20', 'natura2000_pop': 'float', 'natura2000_povs': 'float', 'anc': 'int', 'anc_area': 'float',
#                 'rp': 'int', 'sanitary_protection_zone': 'str:25', 'tvpv': 'int', 'ot_nat': 'int', 'ot_nat_area': 'float', 
#                 'irrigation': 'int', 'irrigation_source': 'int', 'irrigation_type': 'int', 'jpaid': 'str:13'}, 
#                 'geometry': 'Unknown'}
COLUMNS = {
    'land_use_id': 'id',
    'home_name': 'home_name',
    'area': 'area',
    'perim': 'perimeter',
    'slope': 'slope',
    'z_avg': 'z_avg',
    'eligibility_coef': 'eligibility_coef',
    'mines_status': 'mines_status', 
    'mines_year_removed': 'mines_year_removed', 
    'water_protect_zone': 'water_protect_zone', 
    'natura2000': 'natura2000', 
    'natura2000_ok': 'natura2000_ok', 
    'natura2000_pop': 'natura2000_pop', 
    'natura2000_povs': 'natura2000_povs', 
    'anc': 'anc', 
    'anc_area': 'anc_area', 
    'rp': 'rp', 
    'sanitary_protection_zone': 'sanitary_protection_zone', 
    'tvpv': 'tvpv', 
    'ot_nat': 'ot_nat', 
    'ot_nat_area': 'ot_nat_area', 
    'irrigation': 'irrigation', 
    'irrigation_source': 'irrigation_source', 
    'irrigation_type': 'irrigation_type', 
    'jpaid': 'jpaid',
    'geometry': 'geometry',


}

# Fiboa's id is in string but "land_use_id" from the data is in float.
COLUMN_MIGRATIONS = {
    "land_use_id": lambda column: str(column)
}

MIGRATION = None

FILE_MIGRATION = None

MISSING_SCHEMAS = {
    'required': ['home_name', 'slope', 'z_avg', 'eligibility_coef', 'mines_status', 'mines_year_removed', 'water_protect_zone',
                'natura2000', 'natura2000_ok', 'natura2000_pop', 'natura2000_povs', 'anc', 'anc_area', 'rp', 'sanitary_protection_zone',
                'tvpv', 'ot_nat', 'ot_nat_area', 'irrigation', 'irrigation_source', 'irrigation_type', 'jpaid'],
    'properties': {
        'home_name': {
            'type': 'string:60'  # String with max length of 60 characters
        },
        'slope': {
            'type': 'float'
        },
        'z_avg': {
            'type': 'float'
        },
        'eligibility_coef': {
            'type': 'float'
        },
        'mines_status': {
            'type': 'string:1'  # String with max length of 1 character
        },
        'mines_year_removed': {
            'type': 'int'
        },
        'water_protect_zone': {
            'type': 'string:10'  # String with max length of 10 characters
        },
        'natura2000': {
            'type': 'float'
        },
        'natura2000_ok': {
            'type': 'string:20'  # String with max length of 20 characters
        },
        'natura2000_pop': {
            'type': 'float'
        },
        'natura2000_povs': {
            'type': 'float'
        },
        'anc': {
            'type': 'int'
        },
        'anc_area': {
            'type': 'float'
        },
        'rp': {
            'type': 'int'
        },
        'sanitary_protection_zone': {
            'type': 'string:25'  # String with max length of 25 characters
        },
        'tvpv': {
            'type': 'int'
        },
        'ot_nat': {
            'type': 'int'
        },
        'ot_nat_area': {
            'type': 'float'
        },
        'irrigation': {
            'type': 'int'
        },
        'irrigation_source': {
            'type': 'int'
        },
        'irrigation_type': {
            'type': 'int'
        },
        'jpaid': {
            'type': 'string:13'  # String with max length of 13 characters
        }
    }
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
        missing_schemas=MISSING_SCHEMAS,
        column_migrations=COLUMN_MIGRATIONS,
        layer_filter=LAYER_FILTER,
        migration=MIGRATION,
        file_migration=FILE_MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )
