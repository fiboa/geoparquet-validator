import re
import os
import requests

from .commons.data import read_data_csv
from ..convert_utils import BaseConverter
from .es import code_filter, ADD_COLUMNS, EXTENSIONS

regex = re.compile(r"\d+_(RECFE|BURGOS).*\.shp$")


class ESCatConverter(BaseConverter):
    id = "es_cl"
    short_name = "Spain Castilla y León"
    title = "Spain Castile and León Crop fields"
    description = """
    Official SIGPAC land plan for the year 2024. (reference date 02-01-2024)

    Source: SIGPAC (FEGA) database. The Land Consolidation Replacement Farms are included,
    not updated in the SIGPAC published in the Viewer.
    Data manager: Ministry of Agriculture, Fisheries and Food.
    Data provided by: Department of Agriculture, Livestock and Rural Development. Regional Government of Castile and Leon.
    Free use of the data is permitted, but commercial exploitation is prohibited.
    """
    providers = [
        {
            "name": "Department of Agriculture, Livestock and Rural Development. Regional Government of Castile and Leon",
            "url": "https://www.itacyl.es/",
            "roles": ["producer", "licensor"]
        }
    ]
    license = "CC-NC"  # Free use of the data is permitted, but commercial exploitation is prohibited.
    columns = {
        "DN_OID": "id",
        "geometry": "geometry",
        "determination_datetime": "determination_datetime",
        "USO_SIGPAC": "crop:code",
        "crop:name": "crop:name",
        "crop:name_en": "crop:name_en",
    }
    column_additions = ADD_COLUMNS | {
        "determination_datetime": "2024-01-01T00:00:00Z"
    }
    extensions = EXTENSIONS
    column_filters = {"USO_SIGPAC": code_filter}

    def migrate(self, gdf):
        gdf.geometry = gdf.geometry.force_2d()
        # This actually is a land use code. Not sure if we should put this in crop:code
        rows = read_data_csv("es_coda_uso.csv")
        mapping = {row["original_code"]: row["original_name"] for row in rows}
        mapping_en = {row["original_code"]: row["name_en"] for row in rows}
        gdf['crop:name'] = gdf['USO_SIGPAC'].map(mapping)
        gdf['crop:name_en'] = gdf['USO_SIGPAC'].map(mapping_en)
        return gdf

    def download_files(self, uris, cache_folder=None):
        paths = super().download_files(uris, cache_folder)
        new = []
        for path, uri in paths:
            directory = os.path.dirname(path)
            print(directory, os.listdir(directory))
            ps = [z for z in os.listdir(directory) if regex.search(z)]
            assert len(ps), f"Missing matching shapefile in {directory}"
            for p in ps:
                new.append((os.path.join(directory, p), uri))
        return new

    def get_urls(self):
        assert (self.variant or "None") in "2024 2023 2022 2021 2020 2019", f"Wrong variant {self.variant}"
        base = f"http://ftp.itacyl.es/cartografia/05_SIGPAC/{self.variant}_ETRS89/Parcelario_SIGPAC_CyL_Provincias/"
        response = requests.get(base)
        assert response.status_code == 200, f"Error getting urls {response}\n{response.content}"
        uris = {f"{base}{g}": ["replaceme.zip"] for g in re.findall(r'href="(\w+.zip)"', response.text)}
        return uris
