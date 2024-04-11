# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Support for enums and GeoParquet structs
- `fiboa convert`: Allow data of the GeoDataFrame or individual columns to be changed via custom functions

### Fixed

- `fiboa create-geoparquet`: Handle column conversion more gracefully
- `fiboa validate`: Don't fail collection test if something unexpected happened
- `fiboa create-geojson`: Option `-f` doesn't need a value any longer
- `fiboa convert`: Fixed invalid method call

## [v0.3.0] - 2024-04-10

### Added

- Command to validate the fiboa schemas (`fiboa validate-schema`)
- Command to create GeoJSON from GeoParquet (`fiboa create-geojson`)
- Converter for Austria (`at`)
- Converter for Berlin/Brandenburg, Germany (`de_bb`)
- Converter for Schleswig Holstein, Germany (`de_sh`)
- Converter for Lower Saxony, Germany (`de_nds`)

### Changed

- Renamed `fiboa create` to `fiboa create-geoparquet`
- The `--collection` parameter is not needed anylonger if the collection can be
  read directly from the GeoJSON files
  (`fiboa` property or link with relation type `collection`)

### Fixed

- Several minor improvements for the conversion process

## [v0.2.1] - 2024-04-02

### Fixed

- Fixed the field boundary generation for de-nrw, which was pointing at the wrong dataset.

## [v0.2.0] - 2024-04-02

### Added

- Converter framework (`fiboa convert`)
- Converter for North Rhine-Westphalia, Germany (`de_nrw`)

### Fixed

- Validator for GeoParquet recognizes missing fields
- `--json` option for describe command doesn't throw error

## [v0.1.1] - 2024-03-27

- Add experimental data validation support for GroParquet files

## [v0.1.0] - 2024-03-27

- Add `describe` command to inspect fiboa GeoParquet files
- Add `jsonschema` command to create JSON Schema from fiboa schema
- Add validateion for GeoJSON

## [v0.0.9] - 2024-02-28

- Support string enums

## [v0.0.8] - 2024-02-28

- Fixed reading GeoJSON FeatureCollections

## [v0.0.7] - 2024-02-23

- Allow folders to be specified as input files [#3](https://github.com/fiboa/cli/issues/3)

## [v0.0.6] - 2024-02-23

- Add `-e` option for create command to support extension schema mapping to local files

## [v0.0.5] - 2024-02-23

- Add `-e` option for validate command to support extension schema mapping to local files

## [v0.0.4] - 2024-02-23

- Adds missing dependencies

## [v0.0.3] - 2024-02-23

- Use extension schemas for conversion
- Correctly write the Parquet schema and columns - workaround for <https://github.com/geopandas/geopandas/issues/3182>

## [v0.0.2] - 2024-02-16

- Basic validation for collection
- Minimal validation for data
- Fixed creation of GeoParquet files

## [v0.0.1] - 2024-02-16

- First release

[Unreleased]: <https://github.com/fiboa/cli/compare/v0.3.1...main>
[v0.3.1]: <https://github.com/fiboa/cli/compare/v0.3.0...v0.3.1>
[v0.3.0]: <https://github.com/fiboa/cli/compare/v0.2.1...v0.3.0>
[v0.2.1]: <https://github.com/fiboa/cli/compare/v0.2.0...v0.2.1>
[v0.2.0]: <https://github.com/fiboa/cli/compare/v0.1.1...v0.2.0>
[v0.1.1]: <https://github.com/fiboa/cli/compare/v0.1.0...v0.1.1>
[v0.1.0]: <https://github.com/fiboa/cli/compare/v0.0.9...v0.1.0>
[v0.0.9]: <https://github.com/fiboa/cli/compare/v0.0.8...v0.0.9>
[v0.0.8]: <https://github.com/fiboa/cli/compare/v0.0.7...v0.0.8>
[v0.0.7]: <https://github.com/fiboa/cli/compare/v0.0.6...v0.0.7>
[v0.0.6]: <https://github.com/fiboa/cli/compare/v0.0.5...v0.0.6>
[v0.0.5]: <https://github.com/fiboa/cli/compare/v0.0.4...v0.0.5>
[v0.0.4]: <https://github.com/fiboa/cli/compare/v0.0.3...v0.0.4>
[v0.0.3]: <https://github.com/fiboa/cli/compare/v0.0.2...v0.0.3>
[v0.0.2]: <https://github.com/fiboa/cli/compare/v0.0.1...v0.0.2>
[v0.0.1]: <https://github.com/fiboa/cli/tree/v0.0.1>
