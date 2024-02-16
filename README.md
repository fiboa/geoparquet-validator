# fiboa CLI

CLI tools such as validation and file format conversion for fiboa.

## Installation

You need Python 3.9+ installed. 
Run `pip install fiboa-cli` to install the validator.

## Validation

To validate a fiboa GeoParquet file, you can for example run:

`fiboa validate example.json --collection collection.json`

Check `fiboa validate --help` for more details.

The validator also supports remote files.

- `http://` or `https://`: no further configuration is needed.
- `s3://`: `s3fs` needs to be installed (run `pip install .[s3]`) and you may need to set environment variables.
  Refer [here](https://s3fs.readthedocs.io/en/latest/#credentials) for how to define credentials.
- `gs://`: `gcsfs` needs to be installed (run `pip install .[gcs]`).
  By default, `gcsfs` will attempt to use your default gcloud credentials or, attempt to get credentials from the google metadata service, or fall back to anonymous access.

## Create fiboa GeoParquet from GeoJSON

To create a fiboa-compliant GeoParquet for a fiboa-compliant set of GeoJSON files containing Features or FeatureCollections,
you can for example run:

`fiboa create geojson/example.json -o test.parquet -c geojson/collection.json`

Check `fiboa create --help` for more details.

## Development

To install in development mode run `pip install -e .` in this folder.
