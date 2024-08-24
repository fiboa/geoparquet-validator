# Converter for geopackage output of Planet's Field Boundaries dataset.

from ..convert_utils import convert as convert_
import os
import re

SOURCES = None

DATA_ACCESS = """
Data must be obtained from the Planet subscriptions API, see
https://developers.planet.com/docs/planetary-variables/field-boundaries/ for additional information.
The output should look something like FIELD_BOUNDARIES_v1.0.0_S2_P1M-20230101T000000Z_fb.gpkg
"""

ID = "planet_afb"
SHORT_NAME = "Planet Field Boundaries"
TITLE = "Field boundaries created by Planet's Automated Field Boundary detection algorithm"
DESCRIPTION = """
These field boundaries are created by Planet Labs, using an automated process using satellite imagery. The algorithm
works on a monthly basis and is available for the entire globe. The data is provided in GeoPackage format.
For more information see the [field boundaries technical specification](https://planet.widen.net/s/5vq8w5wjvf/2403.08_mar-9444-field-boundaries-technical-specification-sheet-3)
"""

PROVIDERS = [
    {
        "name": "Planet Labs, PBC",
        "url": "https://planet.com",
        "roles": ["producer", "licensor"]
    }
]
ATTRIBUTION = "© 2024 Planet Labs, PBC"

LICENSE = {
    "title": "Proprietary License",
    "href": "https://www.planet.com/licensing-information/",
    "type": "text/html",
    "rel": "license"
}

EXTENSIONS = [
    "https://fiboa.github.io/planet-extension/v0.1.0/schema.yaml"
]

COLUMNS = {
    "polygon_id": "id", #fiboa core field
    "area_ha": "area", #fiboa core field
    "geometry": "geometry", #fiboa core field
    "determination_datetime": "determination_datetime", #fiboa core field
    "ca_ratio": "planet:ca_ratio", #From Planet extension for fiboa
    "micd": "planet:micd", #From Planet extension for fiboa
    "qa": "planet:qa", #From Planet extension for fiboa
}

ADD_COLUMNS = {
  "determination_method": "auto-imagery"
}

def FILE_MIGRATION(gdf, path, uri, layer = None):
    # The file name contains the date, so we can use that to add a
    # date column to the dataset.
    # Assumed filename:
    # FIELD_BOUNDARIES_v1.0.0_S2_P1M-20230101T000000Z_fb.gpkg
    name = os.path.basename(path)
    matches = re.search(r"-(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z_fb.gpkg$", name)
    if matches:
        dt = matches.groups()
        gdf["determination_datetime"] = f"{dt[0]}-{dt[1]}-{dt[2]}T{dt[3]}:{dt[4]}:{dt[5]}Z"
    return gdf

MISSING_SCHEMAS = {
}


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
        extensions=EXTENSIONS,
        missing_schemas=MISSING_SCHEMAS,
        column_additions=ADD_COLUMNS,
        file_migration=FILE_MIGRATION,
        attribution=ATTRIBUTION,
        license=LICENSE,
        **kwargs
    )
