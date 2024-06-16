from ..convert_utils import convert as convert_
from tempfile import TemporaryDirectory
from ..util import download_file, log
import os

URI = "https://data.geopf.fr/telechargement/download/RPG/RPG_2-0__GPKG_LAMB93_FXX_2022-01-01/RPG_2-0__GPKG_LAMB93_FXX_2022-01-01.7z.001"
ID = "fr"
TITLE = "Registre Parcellaire Graphique; Crop Fields France"
DESCRIPTION = """
France has published Crop Field data for many years. Crop fields are declared by farmers within the Common Agricultural Policy (CAP) subsidy scheme.

The anonymized version is distributed as part of the public service for making reference data available contains graphic data for plots (basic land unit for farmers' declaration) with their main crop. This data has been produced by the Services and Payment Agency (ASP) since 2007.
"""
BBOX = [-6.047022416643922, -3.916364769838749, 68.89050422648864, 51.075100624023094]

PROVIDER_NAME = "Anstitut National de l'Information Géographique et Forestière"
PROVIDER_URL = "https://www.data.gouv.fr/en/datasets/registre-parcellaire-graphique-rpg-contours-des-parcelles-et-ilots-culturaux-et-leur-groupe-de-cultures-majoritaire/"
# Attribution example as described in the open license
ATTRIBUTION = "IGN — Original data downloaded from https://geoservices.ign.fr/rpg, updated on June 14 2024"
LICENSE = {"title": "Licence Ouverte / Open Licence", "href": "https://etalab.gouv.fr/licence-ouverte-open-licence", "type": "text/html", "rel": "license"}

COLUMNS = {
    'geometry': 'geometry',
    'id_parcel': 'id',
    'surf_parc': 'area',
    'code_cultu': 'crop_code',
    'code_group': 'group_code',
}

ADD_COLUMNS = {
    "determination_datetime": "2022-01-15T00:00:00Z"
}
EXTENSIONS = []

COLUMN_MIGRATIONS = {}
COLUMN_FILTERS = {
    "surf_parc": lambda col: col > 0.0  # fiboa validator requires area > 0.0
}
MISSING_SCHEMAS = {
    "properties": {
        "crop_code": {
            "type": "string"
        },
        "group_code": {
            "type": "string"
        },
    }
}


def migrate(gdf):
    # Convert accidental multipolygon type to polygon
    gdf = gdf.explode(index_parts=False)
    return gdf


def convert(output_file, cache_file = None, source_coop_url = None, collection = False, compression = None):
    # We need to extract manually because the GeoPackage is 7zipped
    log("Loading file from: " + URI)
    path = download_file(URI, cache_file)
    import py7zr

    with TemporaryDirectory() as tmp_dir:
        log("Unzipping to: " + tmp_dir)
        with py7zr.SevenZipFile(path, 'r') as f:
            gpkg = next(f for f in f.getnames() if f.endswith('.gpkg'))

            f.extractall(tmp_dir)
            new_path = os.path.join(tmp_dir, gpkg)
            convert_(
                output_file,
                cache_file,
                new_path,
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
                migration=migrate,
                attribution=ATTRIBUTION,
                store_collection=collection,
                license=LICENSE,
                compression=compression,
            )
