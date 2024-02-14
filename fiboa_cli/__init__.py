import click

from .validate import validate as validate_
from .create import create as create_
from .util import log, valid_file_for_cli, valid_files_for_cli
from .version import __version__

@click.group()
@click.version_option(version=__version__)
def cli():
  """
  A simple CLI app.
  """
  pass

@click.command()
@click.argument('files', nargs=-1, callback=valid_files_for_cli)
@click.option('--schema', '-s', type=click.Path(exists=True), help='fiboa Schema to validate against. Can be a local file or a URL. If not provided, uses the fiboa version to load the schema for the released version.')
@click.option('--fiboa-version', '-f', type=click.STRING, help='The fiboa version to validate against. Default is the version given in the collection.', default=None)
@click.option('--collection', '-c', type=click.Path(exists=True), help='Points to the STAC collection that defines the fiboa version and extensions.', default=None)
@click.option('--verbose', '-v', type=click.BOOL, help='Run with verbose logging.', default=False)
def validate(files, schema, fiboa_version, collection, verbose):
  """
  Validates a fiboa GeoParquet file.
  """
  log(f"fiboa CLI {__version__} - Validator\n", "success")
  config = {
    "schema": schema,
    "fiboa_version": fiboa_version,
    "collection": collection,
    "verbose": verbose
  }
  for file in files:
    log(f"Validating {file}", "info")
    try:
      result = validate_(file, config)
      if result:
        log(f"  => VALID\n", "success")
      else:
        log(f"  => INVALID\n", "error")
    except Exception as e:
      log(f"  => UNKNOWN: {e}\n", "error")

@click.command()
@click.argument('files', nargs=-1, callback=valid_files_for_cli)
@click.option('--out', '-o', type=click.Path(exists=False), help='File to write the file to. If not provided, prints the file to the STDOUT.', required=True)
@click.option('--collection', '-c', callback=valid_file_for_cli, help='Points to the STAC collection that defines the fiboa version and extensions.', required=True)
@click.option('--schema', '-s', type=click.Path(exists=True), help='fiboa Schema to work against. If not provided, uses the fiboa version from the collection to load the schema for the released version.')
def create(files, out, collection, schema):
  """
  Create a fiboa file from GeoJSON file(s).
  """
  config = {
    "files": files,
    "out": out,
    "schema": schema,
    "collection": collection
  }
  create_(config)

cli.add_command(validate)
cli.add_command(create)

if __name__ == '__main__':
  cli()
