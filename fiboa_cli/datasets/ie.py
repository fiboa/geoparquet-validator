import os

from ..convert_gml import gml_assure_columns
from ..util import log
import geopandas

from ..convert_utils import convert as convert_

SOURCES = {
  "https://osi-inspire-atom.s3-eu-west-1.amazonaws.com/IACSdata/IACS_GSAA_2022.zip": ["IACS_GSAA_2022.gml"]
}
LAYER = "ExistingLandUseObject"

ID = "ie"
SHORT_NAME = "Ireland"
TITLE = "Ireland INSPIRE Geospatial aid application (GSAA) dataset"
DESCRIPTION = """
This data represents the outline shape of LPIS parcels as claimed under area based schemes. The dataset includes the crops claimed as part of the annual GSAA.
Yearly information provided through the beneficiary declaration.
"""

PROVIDERS = [
    {
        "name": "Ireland Department of Agriculture, Food and the Marine",
        "url": "https://inspire.geohive.ie/geoportal/",
        "roles": ["licensor", "distributor"]
    }
]
ATTRIBUTION = "Ireland Department of Agriculture, Food and the Marine"
LICENSE = {
    "title": "This layer is published under the terms of the license Creative Commons Attribution 4.0 International (CC BY 4.0)",
    "href": "https://creativecommons.org/licenses/by/4.0/",
    "type": "text/html",
    "rel": "license"
}

COLUMNS = {
    "geometry": "geometry",
    "crop_name": "crop_name",
    "localId": "id",
    "determination_datetime": "determination_datetime",
}

MISSING_SCHEMAS = {
    "properties": {
        "crop_name": {
            "type": "string"
        },
    }
}


def migration(gdf):
    gdf['crop_name'] = gdf['crop_name'].str.split(', ').str.get(0)
    gdf = gdf[gdf['crop_name'] != 'Void']
    gdf["determination_datetime"] = gdf["observationDate"].str.replace("+01:00", "T00:00:00Z")
    return gdf


def convert(output_file, cache = None, **kwargs):
    def file_migration(data, path, uri, layer):
        return gml_assure_columns(data, path, uri, layer,
                                  crop_name={"ElementPath": "specificLandUse@title", "Type": "String", "Width": 255})

    convert_(
        output_file,
        cache,
        SOURCES,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        providers=PROVIDERS,
        missing_schemas=MISSING_SCHEMAS,
        migration=migration,
        attribution=ATTRIBUTION,
        license=LICENSE,
        layer_filter=lambda layer, uri: layer == LAYER,
        file_migration=file_migration,
        explode_multipolygon=True,
        **kwargs
    )
