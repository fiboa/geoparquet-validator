from os.path import join, dirname

from .commons.ec import load_ec_mapping
from ..convert_utils import convert as convert_


SOURCES = {
    "https://www.nass.usda.gov/Research_and_Science/Crop-Sequence-Boundaries/datasets/NationalCSB_2016-2023_rev23.zip": ["NationalCSB_2016-2023_rev23/CSB1623.gdb"]
}
ID = "us_usda_cropland"
SHORT_NAME = "USDA CSB"
TITLE = "USDA Crop Sequence Boundaries"
DESCRIPTION = """
The Crop Sequence Boundaries (CSB) developed with USDA's Economic Research Service, produces estimates of field boundaries, crop acreage, and crop rotations across the contiguous United States. It uses satellite imagery with other public data and is open source allowing users to conduct area and statistical analysis of planted U.S. commodities and provides insight on farmer cropping decisions.

NASS needed a representative field to predict crop planting based on common crop rotations such as corn-soy and ERS is using this product to study changes in farm management practices like tillage or cover cropping over time.

CSB represents non-confidential single crop field boundaries over a set time frame. It does not contain personal identifying information. The boundaries captured are of crops grown only, not ownership boundaries or tax parcels (unit of property). The data are from satellite imagery and publicly available data, it does not come from producers or agencies like the Farm Service Agency.
"""

EXTENSIONS = ["https://fiboa.github.io/crop-extension/v0.1.0/schema.yaml"]
PROVIDERS = [
    {
        "name": "United States Department of Agriculture",
        "url": "https://www.nass.usda.gov/",
        "roles": ["licensor", "producer"]
    }
]
LICENSE ={
    "title": "The USDA NASS Crop Sequence Boundaries and the data offered at https://www.nass.usda.gov/Research_and_Science/Crop-Sequence-Boundaries are provided to the public as is and are considered public domain and free to redistribute. Users of the Crop Sequence Boundaries (CSB) are solely responsible for interpretations made from these products. The CSB are provided 'as is' and the USDA NASS does not warrant results you may obtain using the data. Contact our staff at (SM.NASS.RDD.GIB@usda.gov) if technical questions arise.",
    "href": "https://gee-community-catalog.org/projects/csb/#license-and-liability",
    "type": "text/html",
    "rel": "license"
}

COLUMNS = {
    "geometry": "geometry",
    "CSBID": "id",
    'CDL2023': 'crop:code',
    'crop:name': 'crop:name',
    'CNTY': "administrative_area_level_2",
}

ADD_COLUMNS = {
    "determination_datetime": "2023-05-01T00:00:00Z",
    "crop:code_list": "https://github.com/fiboa/cli/blob/main/fiboa_cli/datasets/data-files/us_usda_cropland.csv",
}

MISSING_SCHEMAS = {
    "properties": {
        "administrative_area_level_2": {
            "type": "string"
        },
    }
}


def migrate(gdf):
    # merge adjacent polygons
    # gdf = gdf[gdf["STATEFIPS"] == "29"]
    gdf = gdf.explode()
    gdf.dissolve(by=["CDL2023"], aggfunc="first").explode()
    # gdf['geometry'] = gdf['geometry']. make_valid()
    mapping = load_ec_mapping(url=join(dirname(__file__), "data-files", "us_usda_cropland.csv"))
    original_name_mapping = {int(e["original_code"]): e["original_name"] for e in mapping}
    gdf['crop:name'] = gdf['CDL2023'].map(original_name_mapping)
    return gdf


def convert(output_file, cache = None, **kwargs):
    convert_(
        output_file,
        cache,
        SOURCES,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        migration=migrate,
        column_additions=ADD_COLUMNS,
        extensions=EXTENSIONS,
        providers=PROVIDERS,
        license=LICENSE,
        explode_geometries=True,
        missing_schemas=MISSING_SCHEMAS,
        **kwargs
    )
