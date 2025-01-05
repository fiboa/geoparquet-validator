from ..convert_utils import convert as convert_
import pandas as pd

YEARS = {
    2021: "https://data.source.coop/pacificspatial/field-polygon-jp/parquet/jp_field_polygons_2021.parquet",
    2022: "https://data.source.coop/pacificspatial/field-polygon-jp/parquet/jp_field_polygons_2022.parquet",
    2023: "https://data.source.coop/pacificspatial/field-polygon-jp/parquet/jp_field_polygons_2023.parquet",
    2024: "https://data.source.coop/pacificspatial/field-polygon-jp/parquet/jp_field_polygons_2024.parquet",
}

ID = "jp"
SHORT_NAME = "Japan"
TITLE = "Japan Fude Parcels"
DESCRIPTION = """
Japanese Farmland Parcel Polygons (Fude Polygons in Japanese) represent parcel information of farmland.
The polygons are manually digitized data derived from aerial imagery, such as satellite images. Since no
on-site verification or similar procedures have been conducted, the data may not necessarily match the actual
current conditions. Fude Polygons are created for the purpose of roughly indicating the locations of farmland.
"""

PROVIDERS = [
    {
        "name": "Japanese Ministry of Agriculture, Forestry and Fisheries (MAFF, 農林水産省)",
        "url": "https://www.maff.go.jp/",
        "roles": ["licensor", "producer"]
    },
    {
        "name": "Hiroo Imaki, Pacific Spatial Solutions",
        "url": "https://pacificspatial.com/",
        "roles": ["processor"],
    }
]
ATTRIBUTION = "Fude Polygon Data (2021-2024). Japanese Ministry of Agriculture, Forestry and Fisheries. Processed by Pacific Spatial Solutions, Inc"
LICENSE = "CC-BY-4.0"

COLUMNS = {
    "GEOM": "geometry",
    'polygon_uuid': 'id',
    'land_type_en': "land_type_en",
    'local_government_cd': 'admin_local_code',
    "issue_year": "determination_datetime",
}
COLUMN_MIGRATIONS = {
    "issue_year": lambda col: pd.to_datetime(col, format='%Y'),
}

MISSING_SCHEMAS = {
    'properties': {
        'land_type_en': {
            'type': 'string'
        },
        'admin_local_code': {
            'type': 'string'
        },
    }
}


def convert(output_file, cache = None, **kwargs):
    year = 2024  # TODO make parameter
    sources = {YEARS[year]: f"jp_field_polygons_{year}.parquet"}
    convert_(
        output_file,
        cache,
        sources,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        missing_schemas=MISSING_SCHEMAS,
        column_migrations=COLUMN_MIGRATIONS,
        providers=PROVIDERS,
        attribution=ATTRIBUTION,
        license=LICENSE,
        columns=["GEOM", "polygon_uuid", "land_type_en", "local_government_cd", "issue_year"],
        **kwargs
    )
