import click
import os
import yaml
import json
import pyarrow.parquet as pq

from urllib.parse import urlparse
from fsspec import AbstractFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

from .const import LOG_STATUS_COLOR, SUPPORTED_PROTOCOLS

def log(text: str, status="info"):
  """Log a message with a severity level (which leads to different colors)"""
  click.echo(click.style(text, fg=LOG_STATUS_COLOR[status]))

def load_file(uri):
  """Load files from various sources"""
  fs = get_fs(uri)
  with fs.open(uri) as f:
      data = f.read()
 
  if uri.endswith(".yml") or uri.endswith(".yaml"):
    return yaml.safe_load(data)
  elif uri.endswith(".json") or uri.endswith(".geojson"):
    return json.loads(data)
  else:
    return data

def load_parquet_schema(uri: str) -> pq.ParquetSchema:
  """Load schema from Parquet file"""
  fs = get_fs(uri)
  pyarrow_fs = PyFileSystem(FSSpecHandler(fs))
  return pq.read_schema(pyarrow_fs.open_input_file(uri))

def load_fiboa_schema(config):
  """Load fiboa schema"""
  schema_url = config.get('schema')
  schema_version = config.get('fiboa_version')
  if not schema_url:
      schema_url = f"https://fiboa.github.io/specification/v{schema_version}/schema.yaml"
  return load_file(schema_url)

def get_fs(url_or_path: str) -> AbstractFileSystem:
  """Choose fsspec filesystem by sniffing input url"""
  parsed = urlparse(url_or_path)

  if parsed.scheme == "http" or parsed.scheme == "https":
    return HTTPFileSystem()

  if parsed.scheme == "s3":
    from s3fs import S3FileSystem
    return S3FileSystem()

  if parsed.scheme == "gs":
    from gcsfs import GCSFileSystem
    return GCSFileSystem()

  return LocalFileSystem()

def is_valid_file_uri(input):
  """Determine if the input is a file path or a URL and handle it."""
  if os.path.exists(input):
    return input
  elif is_valid_url(input):
    return input
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

def valid_files_for_cli(ctx, param, value):
  return [is_valid_file_uri(val) for val in value]

def valid_file_for_cli(ctx, param, value):
  return is_valid_file_uri(value)
