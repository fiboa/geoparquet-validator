from ..convert_utils import convert as convert_

URI = "https://service.pdok.nl/rvo/referentiepercelen/atom/downloads/referentiepercelen.gpkg"

ID = "nl"
TITLE = "Field boundaries for The Netherlands"
DESCRIPTION = """
A field block (Dutch: "Referentieperceel"), formerly known as "AAN" (Agrarisch Areaal Nederland),
is a contiguous agricultural area surrounded by permanent boundaries, which is cultivated by one or
more farmers with one or more crops, is fully or partially set aside or is fully or partially
taken out of production.

The following field block types exist:

- Woods (Hout)
- Agricultural area (Landbouwgrond)
- Other (Overig)
- Water (Water)

We filter on "Agricultural area" in this converter.
For crop data, look at BasisRegistratie gewasPercelen (BRP)
"""
BBOX = [2.35417303, 50.71447164, 7.5553525, 55.66948102]

PROVIDER_NAME = "RVO / PDOK"
PROVIDER_URL = "https://www.pdok.nl/introductie/-/article/referentiepercelen"
ATTRIBUTION = None
# Both http://creativecommons.org/publicdomain/zero/1.0/deed.nl and http://creativecommons.org/publicdomain/mark/1.0/
LICENSE = "CC0-1.0"

ADD_COLUMNS = {
    "determination_datetime": "2023-06-15T00:00:00Z"
}

EXTENSIONS = []
COLUMN_MIGRATIONS = {}
COLUMNS = {
    'geometry': 'geometry',
    'id': 'id',
    'area': "area",
    'versiebron': 'source'
}

COLUMN_FILTERS = {
    # type = "Hout" | "Landbouwgrond" | "Overig" | "Water"
    "type": lambda col: col == "Landbouwgrond"
}


def migrate(gdf):
    # Projection is in CRS 28992 (RD New), this is the area calculation method of the source organization
    gdf['area'] = gdf.area / 10000
    # index attribute is available through pyogrio
    gdf['id'] = gdf.index
    # Convert accidental multipolygon type to polygon
    gdf = gdf.explode(index_parts=False)
    return gdf


MIGRATION = migrate

MISSING_SCHEMAS = {
    "properties": {
        "source": {
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
        column_additions=ADD_COLUMNS,
        column_migrations=COLUMN_MIGRATIONS,
        column_filters=COLUMN_FILTERS,
        migration=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
        # pyogrio + fix_as_index allow us to use the fid column
        # see https://github.com/geopandas/geopandas/issues/2794
        engine='pyogrio',
        fid_as_index=True
    )
