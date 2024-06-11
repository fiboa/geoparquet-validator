from ..convert_utils import convert as convert_
import pandas as pd


URI = "https://service.pdok.nl/rvo/brpgewaspercelen/atom/v1_0/downloads/brpgewaspercelen_concept_2023.gpkg"

ID = "nl_crop"
TITLE = "BRP Crop Field Boundaries for The Netherlands (CAP-based)"
DESCRIPTION = """
"BasisRegistratiePercelen" (BRP) combines the location of
agricultural plots with the crop grown. The data set
is published by RVO (Netherlands Enterprise Agency). The boundaries of the agricultural plots
are based within the reference parcels (formerly known as AAN). A user an agricultural plot
annually has to register his crop fields with crops (for the Common Agricultural Policy scheme).
A dataset is generated for each year with reference date May 15.
A view service and a download service are available for the most recent BRP crop plots.

<https://service.pdok.nl/rvo/brpgewaspercelen/atom/v1_0/index.xml>

Data is currently available for the years 2009 to 2023.
"""
BBOX = [2.35417303, 50.71447164, 7.5553525, 55.66948102]

PROVIDER_NAME = "RVO / PDOK"
PROVIDER_URL = "https://www.pdok.nl/introductie/-/article/basisregistratie-gewaspercelen-brp-"
ATTRIBUTION = None
# Both http://creativecommons.org/publicdomain/zero/1.0/deed.nl and http://creativecommons.org/publicdomain/mark/1.0/
LICENSE = "CC0"

EXTENSIONS = []
COLUMN_MIGRATIONS = {}
COLUMNS = {
    'geometry': 'geometry',
    'id': 'id',
    'area': "area",
    'category': 'category',
    'gewascode': 'crop_code',
    'gewas': 'crop_name',
    'determination_datetime': 'determination_datetime'
}

COLUMN_FILTERS = {
    # category = "Grasland" | "Bouwland" | "Sloot" | "Landschapselement"
    "category": lambda col: col.isin(["Grasland", "Bouwland"])
}


def migrate(gdf):
    # Projection is in CRS 28992 (RD New), this is the area calculation method of the source organization
    gdf['area'] = gdf.area / 10000
    # Add 15th of may to original "year" (jaar) column
    gdf['determination_datetime'] = pd.to_datetime(gdf['jaar'], format='%Y') + pd.DateOffset(months=4, days=14)
    # index attribute is available through pyogrio
    gdf['id'] = gdf.index
    return gdf


MIGRATION = migrate

MISSING_SCHEMAS = {
    "properties": {
        "category": {
            "type": "string",
            "enum": ["Grasland", "Bouwland"]
        },
        "crop_name": {
            "type": "string"
        },
        "crop_code": {
            "type": "string"
        },
    }
}


def convert(output_file, cache_file = None, source_coop_url = None, collection = False, compression = None):
    convert_(
        output_file,
        cache_file,
        URI,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        BBOX,
        provider_name=PROVIDER_NAME,
        provider_url=PROVIDER_URL,
        source_coop_url=source_coop_url,
        extensions=EXTENSIONS,
        missing_schemas=MISSING_SCHEMAS,
        column_migrations=COLUMN_MIGRATIONS,
        column_filters=COLUMN_FILTERS,
        migration=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )
