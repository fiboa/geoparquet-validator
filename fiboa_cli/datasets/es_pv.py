from .commons.data import read_data_csv
from ..convert_utils import convert as convert_
import requests
import pandas as pd


ID = "es_pv"
SHORT_NAME = "Spain Basque Country"
TITLE = "Spain Basque Country Crop fields"
DESCRIPTION = """
SIGPAC, the geographic information system for the identification of agricultural plots, is the system
that farmers and ranchers must use to apply for community aid related to the area. The reason for
putting this system into effect was the result of a requirement imposed by the European Union on
all Member States. Sigpac began to be used from February 1, 2005, together with the beginning of
the 2005 community aid application period.
"""
EXTENSIONS = ["https://fiboa.github.io/crop-extension/v0.1.0/schema.yaml"]
PROVIDERS = [
    {
        "name": "Basque Government",
        "url": "https://www.euskadi.eus/gobierno-vasco/inicio/",
        "roles": ["producer", "licensor"]
    }
]
ATTRIBUTION = "Basque Government"
LICENSE = "CC-BY-4.0"
COLUMNS = {
    "geometry": "geometry",
    "id": "id",
    "CAMPANA": "determination_datetime",
    "crop:code": "crop:code",
    "crop:name": "crop:name",
}

COLUMN_MIGRATIONS = {
    'CAMPANA': lambda col: pd.to_datetime(col, format='%Y')
}
COLUMN_FILTERS = {
    'USO': lambda col: ~col.isin("AG/CA/ED/FO/IM/IS/IV/TH/ZC/ZU/ZV".split("/")),
}
ADD_COLUMNS = {
    # https://www.euskadi.eus/contenidos/informacion/pac2015_pagosdirectos/es_def/adjuntos/Anexos_PAC_marzo2015.pdf
    # https://www.fega.gob.es/sites/default/files/files/document/AD-CIRCULAR_2-2021_EE98293_SIGC2021.PDF
    # Very generic list
    "crop:code_list": "https://github.com/fiboa/cli/blob/main/fiboa_cli/datasets/data-files/es_coda_uso.csv",
}


def migrate(gdf):
    # This actually is a land use code. Not sure if we should put this in crop:code
    rows = read_data_csv("es_coda_uso.csv")
    mapping = {row["original_code"]: row["original_name"] for row in rows}
    mapping_en = {row["original_code"]: row["name_en"] for row in rows}
    gdf['crop:code'] = gdf['USO']
    gdf['crop:name'] = gdf['USO'].map(mapping)
    gdf['crop:name_en'] = gdf['USO'].map(mapping_en)
    return gdf


def convert(output_file, cache = None, **kwargs):
    from bs4 import BeautifulSoup

    year = 2024  # TODO make variable, 2016...2024

    # Parse list of zips in two steps from source url
    host = "https://www.geo.euskadi.eus"
    base = f"/cartografia/DatosDescarga/Agricultura/SIGPAC/SIGPAC_CAMPA%C3%91A_{year}_V1/"
    soup = BeautifulSoup(requests.get(f"{host}/{base}").content, "html.parser")
    pages = [p['href'] for p in soup.find_all("a") if p['href'].startswith(base)]
    parsed = [BeautifulSoup(requests.get(f"{host}/{page}").content, "html.parser") for page in pages]
    zips = [p['href'] for soup in parsed for p in soup.find_all("a") if p['href'].endswith(".zip")]
    sources = {f"{host}{z}": z.rsplit("/", 1)[1] for z in zips}

    convert_(
        output_file,
        cache,
        sources,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        migration=migrate,
        column_filters=COLUMN_FILTERS,
        providers=PROVIDERS,
        column_migrations=COLUMN_MIGRATIONS,
        column_additions=ADD_COLUMNS,
        attribution=ATTRIBUTION,
        license=LICENSE,
        extensions=EXTENSIONS,
        index_as_id=True,
        **kwargs
    )
