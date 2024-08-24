# Digifarm converter for fiboa
# First draft just takes in a file saved from the API.
# It'd be ideal if there was a CLI that took a Digifarm token and BBOX and would save the file locally and
# convert it. Not sure if we want to keep loading functionality into the converter, if we have a new
# CLI tool that could query field boundary API's - I'd see DigiFarm and Onesoil as options, where you could
# do like 'fiboa api-request digifarm --bbox 4,12,5,13 --token blah | fiboa convert digifarm -i -'.

from ..convert_utils import convert as convert_

SOURCES = None
DATA_ACCESS = """
Data must be obtained from the Digifarm API, see https://api-docs.digifarm.io/.
Use the `-i` CLI parameter to provide the data source.
Provide a URL to an API request (e.g. `https://api.digifarm.io/v1/delineated-fields?token=...&bbox=11.13,60.72,11.21,60.76`)
or download the API response to a local GeoJSON file (use `.json` as file extension).
"""

ID = "digifarm"
SHORT_NAME = "DigiFarm"
TITLE = "Field boundaries created by DigiFarm Automatic Field Delineation Model"
DESCRIPTION = """These field boundaries are created by DigiFarm, using an  state-of-the-art deep neural network model for Field Delineation
from super-resolved satellite imagery. The results are available through an API, covering over 200 million hectares across 30+ countries.
The data is provided through the DigiFarm API at https://api-docs.digifarm.io/, as GeoJSON. For more information see https://digifarm.io/products/field-boundaries
"""
PROVIDERS = [
    {
        "name": "DigiFarm",
        "url": "https://digifarm.io",
        "roles": ["producer", "licensor"]
    }
]
ATTRIBUTION = "© 2024 digifarm.io"
LICENSE = {"title": "DigiFarm Terms and Conditions", "href": "https://digifarm.io/legal/tc", "type": "text/html", "rel": "license"}

COLUMNS = {
    "id": "id",
    "geometry": "geometry",
    "area": "area"
}
COLUMN_MIGRATIONS = {
    'area': lambda column: column / 10000
}

ADD_COLUMNS = {
  "determination_method": "auto-imagery"
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
        column_additions=ADD_COLUMNS,
        column_migrations=COLUMN_MIGRATIONS,
        attribution=ATTRIBUTION,
        license=LICENSE,
        **kwargs
    )
