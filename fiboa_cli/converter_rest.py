from urllib.parse import urlencode

import requests

from .util import get_fs

import os
import geopandas as gpd
import pandas as pd


class EsriRESTConverterMixin:
    cache_folder = None
    rest_base_url = None
    rest_params = {}
    rest_attribute = "OBJECTID"  # orderable, filterable, indexed

    def rest_layer_filter(self, layers):
        return next(iter(layers))

    def get_urls(self, **kwargs):
        assert self.rest_base_url, "Either define {c}.rest_base_url or override {c}.get_urls()".format(c=self.__class__.__name__)
        return {"REST": self.rest_base_url}

    def download_files(self, uris, cache_folder=None):
        # Read-data will just stream alle pages of rest-service
        if next(iter(uris), "").startswith("REST"):
            self.cache_folder = cache_folder
            return list(uris.values())

        # This happens when input_file param is used
        return super().download_files(uris, cache_folder)

    def read_data(self, paths, **kwargs):
        if not paths[0].startswith("http"):
            # This happens when input_file param is used
            return super().read_data(paths, **kwargs)

        base_url = paths[0]  # loop over paths to support more than 1 source

        source_fs = get_fs(base_url)
        cache_fs = None

        if self.cache_folder:
            cache_fs = get_fs(self.cache_folder)
            if not cache_fs.exists(self.cache_folder):
                cache_fs.makedirs(self.cache_folder)

        service_metadata = requests.get(base_url, {"f": "pjson"}).json()
        layer = self.rest_layer_filter(service_metadata["layers"])
        page_size = service_metadata["maxRecordCount"]
        layer_url = f"{base_url}/{layer['id']}/query"
        get_dict = self.rest_params | {
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
            "sortBy": self.rest_attribute,
            "resultRecordCount": page_size
        }
        gdfs = []
        last_id = -1
        while True:
            get_dict["where"] = f"{self.rest_attribute}>{last_id}"
            url = f"{layer_url}?{urlencode(get_dict)}"
            if cache_fs is not None:
                cache_file = os.path.join(self.cache_folder, f"{self.id}_{layer['id']}_{last_id}.geojson")
                if not cache_fs.exists(cache_file):
                    with cache_fs.open(cache_file, mode='wb') as file:
                        print(url)
                        stream_file(source_fs, url, file)
                url = cache_file

            data = gpd.read_file(url)
            print(f"Read {len(data)} features, page {len(gdfs)} from [{data.iloc[0, 0]} ... {data.iloc[-1, 0]}]")
            last_id = data[self.rest_attribute].values[-1]

            # 0. Run migration per file/layer
            data = self.file_migration(data, base_url, base_url, layer["id"])
            if not isinstance(data, gpd.GeoDataFrame):
                raise ValueError("Per-file/layer migration function must return a GeoDataFrame")

            gdfs.append(data)
            if not len(data) >= page_size:
                break
        return pd.concat(gdfs)
