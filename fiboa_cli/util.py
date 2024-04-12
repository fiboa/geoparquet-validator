import click
import os
import yaml
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import re

from urllib.parse import urlparse
from fsspec import AbstractFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from pyarrow import NativeFile
from pyarrow.fs import FSSpecHandler, PyFileSystem
from tempfile import NamedTemporaryFile
from typing import Union

from .const import LOG_STATUS_COLOR, SUPPORTED_PROTOCOLS
from .geopandas import decode_metadata, arrow_to_geopandas

small_file_cache = {}
big_file_cache = {}

def log(text: str, status="info"):
    """Log a message with a severity level (which leads to different colors)"""
    click.echo(click.style(text, fg=LOG_STATUS_COLOR[status]))


def stream_file(fs, src_uri, dst_file, chunk_size = 10 * 1024 * 1024):
    with fs.open(src_uri, mode='rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            dst_file.write(chunk)


def download_file(uri, cache_file = None):
    """Download files from various sources"""
    if uri not in big_file_cache:
        source_fs = get_fs(uri)
        if isinstance(source_fs, LocalFileSystem):
            big_file_cache[uri] = uri
        else:
            if cache_file is not None:
                cache_fs = get_fs(cache_file)
                if not cache_fs.exists(cache_file):
                    with cache_fs.open(cache_file, mode='wb') as file:
                        stream_file(source_fs, uri, file)

                path = cache_file
            else:
                _, extension = os.path.splitext(uri)
                with NamedTemporaryFile(delete=False, suffix=extension) as tmp_file:
                    stream_file(source_fs, uri, tmp_file)

                path = tmp_file.name

            big_file_cache[uri] = path

    return big_file_cache[uri]


def load_file(uri):
    """Load files from various sources"""
    if uri in small_file_cache:
        return small_file_cache[uri]

    fs = get_fs(uri)

    with fs.open(uri) as f:
        data = f.read()

    if uri.endswith(".yml") or uri.endswith(".yaml"):
        data = yaml.safe_load(data)
    elif uri.endswith(".json") or uri.endswith(".geojson"):
        data = json.loads(data)

    small_file_cache[uri] = data

    return data

def get_pyarrow_file(uri) -> NativeFile:
    fs = get_fs(uri)
    pyarrow_fs = PyFileSystem(FSSpecHandler(fs))
    return pyarrow_fs.open_input_file(uri)


def load_parquet_schema(uri: Union[str, NativeFile]) -> pq.ParquetSchema:
    """Load schema from Parquet file"""
    if isinstance(uri, str):
        uri = get_pyarrow_file(uri)
    return pq.read_schema(uri)


def load_parquet_data(uri: str, nrows = None) -> pd.DataFrame:
    """Load data from Parquet file"""
    f = get_pyarrow_file(uri)

    if nrows is None:
        table = pq.read_table(f)
    else:
        pf = pq.ParquetFile(f)
        rows = next(pf.iter_batches(batch_size = nrows))
        table = pa.Table.from_batches([rows])

    return arrow_to_geopandas(table)


def load_fiboa_schema(config):
    """Load fiboa schema"""
    schema_url = config.get('schema')
    schema_version = config.get('fiboa_version')
    if not schema_url:
        schema_url = f"https://fiboa.github.io/specification/v{schema_version}/schema.yaml"
    return load_file(schema_url)


def load_datatypes(version):
    # todo: allow to define a seperate schema from a file (as in load_fiboa_schema)
    dt_url = f"https://fiboa.github.io/specification/v{version}/geojson/datatypes.json"
    response = load_file(dt_url)
    return response["$defs"]


def get_fs(url_or_path: str) -> AbstractFileSystem:
    """Choose fsspec filesystem by sniffing input url"""
    parsed = urlparse(url_or_path)

    if parsed.scheme in ('http', 'https'):
        return HTTPFileSystem()

    if parsed.scheme == "s3":
        from s3fs import S3FileSystem
        return S3FileSystem()

    if parsed.scheme == "gs":
        from gcsfs import GCSFileSystem
        return GCSFileSystem()

    return LocalFileSystem()


def is_valid_file_uri(uri, extensions = []):
    """Determine if the input is a file path or a URL and handle it."""
    if not isinstance(uri, str):
        return None
    elif len(extensions) > 0 and not uri.endswith(tuple(extensions)):
        return None
    elif os.path.exists(uri):
        return uri
    elif is_valid_url(uri):
        return uri
    else:
        raise click.BadParameter('Input must be an existing local file or a URL with protocol: ' + ",".join(SUPPORTED_PROTOCOLS))


def is_valid_url(url):
    """Check if a URL is valid."""
    try:
        result = urlparse(url)
        return all([
            result.scheme in SUPPORTED_PROTOCOLS,
            result.netloc
        ])
    except:
        return False


def valid_files_folders_for_cli(value, extensions = []):
    files = []
    for v in value:
        v = is_valid_file_uri(v)
        if os.path.isdir(v):
            for f in os.listdir(v):
                if len(extensions) > 0 and not f.endswith(tuple(extensions)):
                    continue
                if f == "collection.json" or f == "catalog.json": # likely STAC
                    continue
                files.append(os.path.join(v, f))
        else:
            files.append(v)
    return files


def valid_file_for_cli(ctx, param, value):
    return is_valid_file_uri(value)


def valid_file_for_cli_with_ext(value, extensions):
    return is_valid_file_uri(value, extensions)


def valid_folder_for_cli(ctx, param, value):
    """Determine if the input is a folder."""
    if os.path.exists(value) and os.path.isdir(value):
        return value
    else:
        raise click.BadParameter('Input must be an existing local folder')


def get_collection(data, collection_path = None, basepath = None):
    # If the user provided a collection, enforce using it
    if collection_path is not None:
        return load_file(collection_path)

    # Look if the data contains a fiboa property
    if "fiboa" in data:
        return data.get("fiboa")

    # Look for a collection link in the data and load the collection from there
    links = data.get("links", [])
    for link in links:
        media_type = link.get("type")
        if link.get("rel") == "collection" and (media_type is None or media_type == "application/json"):
            href = link.get("href")
            if basepath is not None:
                href = os.path.join(os.path.dirname(basepath), href)
            return load_file(href)

    # No collection found
    return None


def check_ext_schema_for_cli(ctx, param, value):
    map = {}
    for v in value:
        try:
            remote, local = v.split(",")
            map[remote] = local
        except ValueError:
            raise click.BadParameter('Extension schema must be a URL and a local file path separated by a comma character')

    return map


def merge_schemas(*schemas):
    """Merge multiple schemas into one"""
    result = {
        "required": [],
        "properties": {}
    }
    for schema in schemas:
        schema = migrate_schema(schema)
        result["required"] += schema.get("required", [])
        result["properties"].update(schema.get("properties", {}))

    return result


def migrate_schema(schema):
    """Migrate schema to a new version"""
    return schema.copy()


def parse_metadata(schema, key):
    if key in schema.metadata:
        return decode_metadata(schema.metadata[key])
    else:
        log(f"Parquet file schema does not have a '{key}' key", "warning")
        return None


def to_iso8601(dt):
    iso = dt.isoformat()
    if iso.endswith("+00:00"):
        return iso[:-6] + "Z"
    elif re.search(r"[+-]\d{2}:\d{2}$", iso):
        raise ValueError("Timezone offset is not supported")
    else:
        return iso + "Z"
