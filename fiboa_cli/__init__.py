import click
import sys

from .validate import validate as validate_
from .create import create as create_
from .util import log, check_ext_schema_for_cli, valid_file_for_cli, valid_files_folders_for_cli
from .version import __version__

@click.group()
@click.version_option(version=__version__)
def cli():
    """
    A simple CLI app.
    """
    pass

@click.command()
@click.argument('files', nargs=-1, callback=lambda ctx, param, value: valid_files_folders_for_cli(value, ["parquet", "geoparquet"]))
@click.option(
    '--schema', '-s',
    type=click.Path(exists=True),
    help='fiboa Schema to validate against. Can be a local file or a URL. If not provided, uses the fiboa version to load the schema for the released version.'
)
@click.option(
    '--ext-schema', '-e',
    multiple=True,
    callback=check_ext_schema_for_cli,
    help='Maps a remote fiboa extension schema url to a local file. First the URL, then the local file path. Separated with a comma character. Example: https://example.com/schema.json,/path/to/schema.json',
)
@click.option(
    '--fiboa-version', '-f',
    type=click.STRING,
    help='The fiboa version to validate against. Default is the version given in the collection.',
    default=None
)
@click.option(
    '--collection', '-c',
    type=click.Path(exists=True),
    help='Points to the STAC collection that defines the fiboa version and extensions.',
    default=None
)
@click.option(
    '--data', '-d',
    is_flag=True,
    type=click.BOOL,
    help='Validate the data in the file. Enabling this might be slow. Default is False.',
    default=False
)
def validate(files, schema, ext_schema, fiboa_version, collection, data):
    """
    Validates a fiboa GeoParquet file.
    """
    log(f"fiboa CLI {__version__} - Validator\n", "success")
    config = {
        "schema": schema,
        "extension_schemas": ext_schema,
        "fiboa_version": fiboa_version,
        "collection": collection,
        "data": data,
    }
    for file in files:
        log(f"Validating {file}", "info")
        try:
            result = validate_(file, config)
            if result:
                log("  => VALID\n", "success")
            else:
                log("  => INVALID\n", "error")
                sys.exit(1)
        except Exception as e:
            log(f"  => UNKNOWN: {e}\n", "error")
            sys.exit(2)

@click.command()
@click.argument('files', nargs=-1, callback=lambda ctx, param, value: valid_files_folders_for_cli(value, ["json", "geojson"]))
@click.option(
    '--out', '-o',
    type=click.Path(exists=False),
    help='File to write the file to. If not provided, prints the file to the STDOUT.',
    required=True
)
@click.option(
    '--collection', '-c',
    callback=valid_file_for_cli,
    help='Points to the STAC collection that defines the fiboa version and extensions.',
    required=True
)
@click.option(
    '--schema', '-s',
    type=click.Path(exists=True),
    help='fiboa Schema to work against. If not provided, uses the fiboa version from the collection to load the schema for the released version.'
)
@click.option(
    '--ext-schema', '-e',
    multiple=True,
    callback=check_ext_schema_for_cli,
    help='Maps a remote fiboa extension schema url to a local file. First the URL, then the local file path. Separated with a comma character. Example: https://example.com/schema.json,/path/to/schema.json',
)
def create(files, out, collection, schema, ext_schema):
    """
    Create a fiboa file from GeoJSON file(s).
    """
    log(f"fiboa CLI {__version__} - Create GeoParquet\n", "success")
    config = {
        "files": files,
        "out": out,
        "schema": schema,
        "collection": collection,
        "extension_schemas": ext_schema,
    }
    try:
        create_(config)
    except Exception as e:
        log(e, "error")
        sys.exit(1)

cli.add_command(validate)
cli.add_command(create)

if __name__ == '__main__':
    cli()
