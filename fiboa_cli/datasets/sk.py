import zipfile
import os
from ..convert_utils import convert as convert_, download_files

SOURCES = {
#    "https://data.slovensko.sk/download?id=ed0b4a21-8774-4bb1-aab3-fea4ddab9010&blocksize=0": "DPB2024_20240912.zip"
    "https://data.slovensko.sk/download?id=e39ad227-1899-4cff-b7c8-734f90aa0b59&blocksize=0": "HU2024_20240917.zip"
}
ID = "sk"
SHORT_NAME = "Slovakia"
TITLE = "Slowakia Agricultural Land Idenfitication System"

DESCRIPTION = """
Systém identifikácie poľnohospodárskych pozemkov (LPIS)

LPIS is an agricultural land identification system. It represents the vector boundaries of agricultural land
and carries information about the unique code, acreage, culture/land use, etc., which is used as a reference
for farmers' applications, for administrative and cross-checks, on-site checks and also checks using remote
sensing methods.

Dataset Hranice užívania contains the use declared by applicants for direct support.
"""
PROVIDERS = [
    {
        "name": "National catalog of open data",
        "url": "https://data.slovensko.sk/",
        "roles": ["producer", "licensor"]
    }
]

LICENSE = "CC-0"  # "Open Data"
COLUMNS = {
    "geometry": "geometry",
    "KODKD": "id",
    "PLODINA": "crop_name",
    "KULTURA_NA": "crop_group",
    "LOKALITA_N": "municipality",
    "VYMERA": "area",
}
COLUMN_MIGRATIONS = {
    "geometry": lambda col: col.make_valid()
}
MISSING_SCHEMAS = {
    "properties": {
        "crop_name": {
            "type": "string"
        },
        "crop_group": {
            "type": "string"
        },
        "municipality": {
            "type": "string"
        },
    }
}


def convert(output_file, cache = None, **kwargs):
    # The zipfile has an embedded directory. This already fails at `gpd.list_layers(path)`
    # Workaround: download + unzip, use unzipped shapefile path as source-place
    assert cache, "Cache is required for this parser"
    download_files(SOURCES, cache)
    path = next(iter(SOURCES.values()))
    with zipfile.ZipFile(os.path.join(cache, path), 'r') as zip_file:
        zip_file.extractall(cache)

    convert_(
        output_file,
        cache,
        os.path.join(cache, path.replace('.zip', 'shp'), path.replace('.zip', '.shp')),
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        providers=PROVIDERS,
        missing_schemas=MISSING_SCHEMAS,
        license=LICENSE,
        column_migrations=COLUMN_MIGRATIONS,
        **kwargs
    )
