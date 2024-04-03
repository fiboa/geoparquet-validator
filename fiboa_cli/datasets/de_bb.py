from ..convert_utils import convert as convert_

# This file consists of three shapefiles (DFBK_FB, DFBK_LE, DFBK_NBF)
# We only want DFBK_FB and for me it loads only this file, but I'm not sure whether that's always the case
URI = "https://data.geobasis-bb.de/geofachdaten/Landwirtschaft/dfbk.zip"
ID = "de_bb"
TITLE = "Field boundaries for Berlin / Brandenburg, Germany"
DESCRIPTION = """A field block (German: "Feldblock") is a contiguous agricultural area surrounded by permanent boundaries, which is cultivated by one or more farmers with one or more crops, is fully or partially set aside or is fully or partially taken out of production."""
# From http://osmtipps.lefty1963.de/2008/10/bundeslnder.html
BBOX = [11.2681664447,51.3606627053,14.7647105012,53.5579500214]
COLUMNS = {
    'geometry': 'geometry',
    'FB_ID': ['flik', 'id'], # make flik id a dedicated column to align with NRW etc.
    'FGUE_JAHR': 'fgue_jahr',
    'FL_BRUTTO_': 'area',
    'FL_NETTO_H': 'net_area',
    'GUELTVON_F': 'determination_datetime',
    'GUELTBIS_F': 'expiry_datetime',
    'KREIS_NR': 'kreis_nr',
    'TK10_BLATT': "tk10_blatt",
    'HBN_KAT': 'hbn',
    'SHAPE_LEN': 'perimeter',
    # Don't include SHAPE_AREA
}
MISSING_SCHEMAS = {
    'flik': {
        'type': 'string',
        'required': True
    },
    'hbn': {
        'type': 'string'
    },
    'fgue_jahr': {
        'type': 'string'
    },
    'net_area': {
        'type': 'float',
        'exclusiveMinimum': 0
    },
    'expiry_datetime':  {
        'type': 'date-time'
    },
    'kreis_nr': {
        'type': 'uint16'
    },
    'tk10_blatt': {
        'type': 'string'
    },
    # todo: remove once we have spec v0.1.1
    'perimeter': {
        'type': 'float',
        'exclusiveMinimum': 0,
        'required': True
    }
}

def convert(output_file, cache_file = None):
    """
    Converts the Berlin/Brandenburg (Germany) field boundary datasets to fiboa.
    """
    convert_(output_file, cache_file, URI, COLUMNS, ID, TITLE, DESCRIPTION, BBOX, missing_schemas=MISSING_SCHEMAS, layer = "DFBK_FB")
