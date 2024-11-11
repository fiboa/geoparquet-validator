from ..convert_utils import convert as convert_


SOURCES = "https://www.apprrr.hr/wp-content/uploads/nipp/land_parcels.gpkg"

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

LICENSE = {"href": "https://www.apprrr.hr/prostorni-podaci-servisi/", "type": "text/html", "rel": "license"}

COLUMNS = {
    'id': 'id',
    'land_use_id': 'land_use_id',
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
    'geometry': 'geometry'
}
COLUMN_MIGRATIONS = {
    'area': lambda column: column / 10000
}

MISSING_SCHEMAS = {
    'required': [
        'mines_status',
        'water_protect_zone',
        'natura2000',
        'sanitary_protection_zone',
        'irrigation',
        'jpaid'
    ],
    'properties': {
        'land_use_id': {
            'type': 'double'
        },
        'home_name': {
            'type': 'string'
        },
        'slope': {
            'type': 'double'
        },
        'z_avg': {
            'type': 'double'
        },
        'eligibility_coef': {
            'type': 'double'
        },
        'mines_status': {
            'type': 'string',
            'enum': [
                'N',
                'M',
                'R'
            ]
        },
        'mines_year_removed': {
            'type': 'int32'
        },
        'water_protect_zone': {
            'type': 'string'
        },
        'natura2000': {
            'type': 'double'
        },
        'natura2000_ok': {
            'type': 'string'
        },
        'natura2000_pop': {
            'type': 'double'
        },
        'natura2000_povs': {
            'type': 'double'
        },
        'anc': {
            'type': 'int32'
        },
        'anc_area': {
            'type': 'double'
        },
        'rp': {
            'type': 'int32'
        },
        'sanitary_protection_zone': {
            'type': 'string'
        },
        'tvpv': {
            'type': 'int32'
        },
        'ot_nat': {
            'type': 'int32'
        },
        'ot_nat_area': {
            'type': 'double'
        },
        'irrigation': {
            'type': 'int32'
        },
        'irrigation_source': {
            'type': 'int32'
        },
        'irrigation_type': {
            'type': 'int32'
        },
        'jpaid': {
            'type': 'string'
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
        column_migrations=COLUMN_MIGRATIONS,
        providers=PROVIDERS,
        missing_schemas=MISSING_SCHEMAS,
        attribution=ATTRIBUTION,
        license=LICENSE,
        index_as_id=True,
        **kwargs
    )
