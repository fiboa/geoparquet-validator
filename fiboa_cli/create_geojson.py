import json
import os
import pandas as pd

from .util import load_parquet_data, load_parquet_schema, parse_metadata, to_iso8601


def create_geojson(file, out, split = False, num = None, indent = None):
    if not os.path.exists(out):
        os.makedirs(out)

    schema = load_parquet_schema(file)
    collection = parse_metadata(schema, b"fiboa")
    geodata = load_parquet_data(file, nrows = num)
    geodata = geodata.to_crs(epsg=4326)

    if split:
        collection_name = "collection.json"
        collection_path = os.path.join(out, collection_name)
        write_json(collection, collection_path, indent)

        i = 1
        for obj in geodata.iterfeatures():
            obj = fix_id(obj)

            links = obj.get("links", [])
            links.append({
                "href": collection_name,
                "rel": "collection",
                "type": "application/json"
            })
            obj["links"] = links

            id = obj.get("id", i)
            path = os.path.join(out, f"{id}.json")
            write_json(obj, path, indent)

            i += 1

    else:
        obj = geodata.__geo_interface__
        del obj["bbox"]
        obj["features"] = list(map(fix_id, obj["features"]))
        obj["fiboa"] = collection
        path = os.path.join(out, "features.json")
        write_json(obj, path, indent)


def write_json(obj, path, indent = None):
    with open(path, "w") as f:
        json.dump(obj, f, allow_nan=False, indent = indent, cls = FiboaJSONEncoder)


def fix_id(obj):
    if "id" in obj["properties"]:
        obj["id"] = obj["properties"]["id"]
    del obj["properties"]["id"]
    return obj


class FiboaJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return to_iso8601(obj)
        else:
            return super().default(obj)
