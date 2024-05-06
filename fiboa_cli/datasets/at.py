from ..convert_utils import convert as convert_
from ..util import download_file, log

from tempfile import TemporaryDirectory
from zipfile import ZipFile

import os

# This file consists of three shapefiles (DFBK_FB, DFBK_LE, DFBK_NBF)
# We only want DFBK_FB and for me it loads only this file, but I'm not sure whether that's always the case
URI = "https://inspire.lfrz.gv.at/009501/ds/inspire_referenzen_2021_polygon.gpkg.zip"
FILENAME = "INSPIRE_REFERENZEN_2021_POLYGON.gpkg"
ID = "at"
TITLE = "Field boundaries for Austria"
DESCRIPTION = """**Field boundaries for Austria - INVEKOS Referenzen Österreich 2021.**

The layer includes all reference parcels ("Referenzparzellen") defined by the paying agency Agrarmarkt Austria and recorded landscape elements (landscape element layers) within the meaning of Art. 5 of Regulation (EU) No. 640/2014 and Regulation of the competent federal ministry with horizontal rules for the area of the Common Agricultural Policy (Horizontal CAP Regulation) StF: Federal Law Gazette II No. 100/2015.

Reference parcel: is the physical block that can be clearly delimited from the outside (e.g. forest, roads, water bodies) and is formed by contiguous agricultural areas that are recognizable in nature."""
PROVIDER_NAME = "Agrarmarkt Austria"
PROVIDER_URL = "https://geometadatensuche.inspire.gv.at/metadatensuche/inspire/api/records/9db8a0c3-e92a-4df4-9d55-8210e326a7ed"
LICENSE = "CC-BY-4.0"
BBOX = [9.527906274165764, 46.41230158735734, 17.15786908837973, 49.021160570100974]
COLUMNS = {
    'geometry': 'geometry',
    'RFL_ID': 'id',
    'REF_ART': 'ref_art',
    'BRUTTOFLAECHE_HA': 'area',
    'INSPIRE_ID': 'inspire:id',
    'REF_ART_BEZEICHNUNG': 'ref_art_bezeichnung',
    'REFERENZ_KENNUNG': 'referenz_kennung',
    'FART_ID': 'fart_id',
    'GEO_DATERF': 'determination_datetime'
}
EXTENSIONS = ["https://fiboa.github.io/inspire-extension/v0.2.0/schema.yaml"]
MISSING_SCHEMAS = {
    'properties': {
        'ref_art': {
            'type': 'string'
        },
        'ref_art_bezeichnung': {
            'type': 'string'
        },
        'referenz_kennung': {
            'type': 'uint64'
        },
        'fart_id': {
            'type': 'uint32'
        }
    }
}

def convert(output_file, cache_file = None, source_coop_url = None, collection = False, compression = None):
    """
    Converts the Austrian field boundary datasets to fiboa.
    """

    # There's a bug in the file that let's the unzipping fail in the normal flow
    log("Loading file from: " + URI)
    path = download_file(URI, cache_file)

    with TemporaryDirectory() as tmp_dir:
        log("Unzipping to: " + tmp_dir)
        with ZipFile(path, 'r') as f:
            f.extractall(tmp_dir)

        new_path = os.path.join(tmp_dir, FILENAME)

        convert_(
            output_file, cache_file, new_path,
            COLUMNS, ID, TITLE, DESCRIPTION, BBOX,
            license=LICENSE,
            extensions = EXTENSIONS,
            missing_schemas=MISSING_SCHEMAS,
            source_coop_url=source_coop_url,
            provider_name=PROVIDER_NAME,
            provider_url=PROVIDER_URL,
            store_collection=collection,
            compression=compression,
        )
