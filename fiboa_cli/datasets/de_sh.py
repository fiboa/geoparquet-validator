from .utils.de_utils import convert as convert_

URI = "https://service.gdi-sh.de/SH_OpenGBD/feeds/Atom_SH_Feldblockfinder_OpenGBD/data/Feldbloecke_2024_GPKG.zip"
ID = "de_sh"
TITLE = "Field boundaries for Schleswig-Holstein (SH), Germany"
DESCRIPTION = """A field block (German: "Feldblock") is a contiguous agricultural area surrounded by permanent boundaries, which is cultivated by one or more farmers with one or more crops, is fully or partially set aside or is fully or partially taken out of production."""
# From http://osmtipps.lefty1963.de/2008/10/bundeslnder.html
BBOX = [7.8685145620,53.3590675115,11.3132037822,55.0573747014]
COLUMNS = {
    'geometry': 'geometry',
    'fachguelti': 'determination_datetime',
    'FLIK': ['flik', 'id'], # make flik id a dedicated column to align with NRW etc.
    'Flaeche': 'area',
    'HBN': 'hbn',
    'SHAPE_LEN': "perimeter"
}
MISSING_SCHEMAS = {
    'flik': {
        'type': 'string',
        'required': True
    },
    'hbn': {
        'type': 'string'
    }
}

def convert(output_file, cache_file = None):
    """
    Converts the Schleswig-Holstein (Germany) field boundary datasets to fiboa.
    """
    convert_(output_file, cache_file, URI, COLUMNS, ID, TITLE, DESCRIPTION, BBOX, missing_schemas=MISSING_SCHEMAS)
