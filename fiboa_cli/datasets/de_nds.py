from ..convert_utils import convert as convert_

URI = "https://sla.niedersachsen.de/mapbender_sla/download/FB_NDS.zip"
ID = "de_nds"
TITLE = "Field boundaries for Lower Saxony, Germany"
DESCRIPTION = """A field block (German: "Feldblock") is a contiguous agricultural area surrounded by permanent boundaries, which is cultivated by one or more farmers with one or more crops, is fully or partially set aside or is fully or partially taken out of production."""
PROVIDER_NAME = "ML/SLA Niedersachsen"
PROVIDER_URL = "https://sla.niedersachsen.de/landentwicklung/LEA/"
ATTRIBUTION = "© ML/SLA Niedersachsen (2024), dl-de/by-2-0 (www.govdata.de/dl-de/by-2-0), Daten bearbeitet"
# From http://osmtipps.lefty1963.de/2008/10/bundeslnder.html
BBOX = [6.6545841239,51.2954150799,11.59769814,53.8941514415]
COLUMNS = {
    'geometry': 'geometry',
    'FLIK': ['id', 'flik'], # make flik id a dedicated column to align with NRW etc.
    'STAND': 'determination_datetime',
    'ANT_JAHR': 'ant_jahr',
    'BNK': 'bnk',
    'BNK_TXT': 'bnk_txt',
    'FLAECHE': 'area',
    'SHAPE_Leng': "perimeter"
    # Don't include SHAPE_Area
}
MISSING_SCHEMAS = {
    'required': ['flik', 'perimeter'],
    'properties': {
        'flik': {
            'type': 'string'
        },
        'ant_jahr': {
            'type': 'int16'
        },
        'bnk': {
            'type': 'string'
        },
        'bnk_txt': {
            'type': 'string'
        },
        # todo: remove once we have spec v0.1.1
        'perimeter': {
            'type': 'float',
            'exclusiveMinimum': 0
        }
    }
}

def convert(output_file, cache_file = None, source_coop_url = None, collection = False):
    """
    Converts the Lower Saxony (Germany) field boundary datasets to fiboa.
    """
    convert_(
        output_file, cache_file,
        URI, COLUMNS, ID, TITLE, DESCRIPTION, BBOX,
        missing_schemas=MISSING_SCHEMAS,
        attribution=ATTRIBUTION,
        source_coop_url=source_coop_url,
        provider_name=PROVIDER_NAME,
        provider_url=PROVIDER_URL,
        store_collection=collection
    )
