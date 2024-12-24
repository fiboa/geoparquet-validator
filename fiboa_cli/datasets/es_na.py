import re

import requests
from os import path
import pandas as pd

from .es import ESBaseConverter


# SOURCES = "https://filescartografia.navarra.es/2_CARTOGRAFIA_TEMATICA/2_6_SIGPAC/" # FULL Download timeout
class NAConverter(ESBaseConverter):
    id = "es_na"
    short_name = "Spain Navarra"
    title = "Spain Navarra Crop fields"
    description = """
    SIGPAC Crop fields of Spain - Navarra
    """
    license = "CC-BY-4.0"  # https://sigpac.navarra.es/descargas/
    attribution = "Comunidad Foral de Navarra"
    providers = [
        {
            "name": "Comunidad Foral de Navarra",
            "url": "https://gobiernoabierto.navarra.es/",
            "roles": ["producer", "licensor"]
        }
    ]
    columns = {
        "geometry": "geometry",
        "BEGINLIFE": "determination_datetime",
        "IDUSO24": "crop:code",
        "crop:name": "crop:name",
        "crop:name_en": "crop:name_en",
    }
    column_migrations = {
        "BEGINLIFE": lambda col: pd.to_datetime(col, format="%d/%m/%Y"),
    }
    use_code_attribute = "IDUSO24"

    def get_urls(self, cache = None, **kwargs):
        # scrape HTML page for sources
        content = requests.get("https://sigpac.navarra.es/descargas/", verify=False).text
        base = re.search('var rutaBase = "(.*?)";', content).group(1)
        last = base.rsplit('/', 1)[-1]
        sources = {f"https://sigpac.navarra.es/descargas/{base}{src}.zip": [f"{last}{src}.shp"] for src in
                   re.findall(r'value:"(\d+)"', content)}

        # Hostname has invalid SSL, hack around cache here
        assert cache, "This parser requires a cache. Use -c <cache_dir>"
        for s in list(sources):
            target = path.join(cache, s.rsplit("/", 1)[1])
            if not path.exists(target):
                r = requests.get(s, verify=False)
                if r.status_code == 200:
                    with open(target, 'wb') as f:
                        f.write(r.content)
                else:
                    sources.pop(s)
