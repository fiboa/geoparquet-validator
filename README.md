# fiboa CLI

CLI tools such as validation and file format conversion for fiboa.

## Installation

You need **Python 3.9+** installed. 
Run `pip install fiboa-cli` to install the validator.

## Validation

To validate a fiboa GeoParquet or GeoJSON file, you can for example run:

- GeoJSON: `fiboa validate example.json --collection collection.json`
- GeoParquet: `fiboa validate example.parquet --data`

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

- `fiboa create geojson/example.json -o test.parquet -c geojson/collection.json`

Check `fiboa create --help` for more details.

## Inspect fiboa GeoParquet file

To look into a fiboa GeoParquet file to get a rough understanding of the content, the following can be executed:

- `fiboa describe example.parquet`

Check `fiboa describe --help` for more details.

## Create JSON Schema from fiboa Schema

To create a JSON Schema for a fiboa Schema YAML file, you can for example run:

- `fiboa jsonschema example.json --id=https://fiboa.github.io/specification/v0.1.0/geojson/schema.json -o schema.json`

Check `fiboa jsonschema --help` for more details.

## Development

To install in development mode run `pip install -e .` in this folder.
