# This file is coming from GeoPandas and is modified to support additional metadata.
# Can be removed once https://github.com/geopandas/geopandas/issues/3182 is fixed.

import json

import numpy as np
from pandas import DataFrame, Series

import shapely

from geopandas._compat import import_optional_dependency
import geopandas
from geopandas.io.file import _expand_user

METADATA_VERSION = "1.0.0"
SUPPORTED_VERSIONS = ["0.1.0", "0.4.0", "1.0.0-beta.1", "1.0.0"]

def _remove_id_from_member_of_ensembles(json_dict):
    """
    Older PROJ versions will not recognize IDs of datum ensemble members that
    were added in more recent PROJ database versions.

    Cf https://github.com/opengeospatial/geoparquet/discussions/110
    and https://github.com/OSGeo/PROJ/pull/3221

    Mimicking the patch to GDAL from https://github.com/OSGeo/gdal/pull/5872
    """
    for key, value in json_dict.items():
        if isinstance(value, dict):
            _remove_id_from_member_of_ensembles(value)
        elif key == "members" and isinstance(value, list):
            for member in value:
                member.pop("id", None)


def _create_metadata(df, schema_version=None):
    """Create and encode geo metadata dict.

    Parameters
    ----------
    df : GeoDataFrame
    schema_version : {'0.1.0', '0.4.0', '1.0.0-beta.1', '1.0.0', None}
        GeoParquet specification version; if not provided will default to
        latest supported version.

    Returns
    -------
    dict
    """

    schema_version = schema_version or METADATA_VERSION

    if schema_version not in SUPPORTED_VERSIONS:
        raise ValueError(
            f"schema_version must be one of: {', '.join(SUPPORTED_VERSIONS)}"
        )

    # Construct metadata for each geometry
    column_metadata = {}
    for col in df.columns[df.dtypes == "geometry"]:
        series = df[col]
        geometry_types = sorted(Series(series.geom_type.unique()).dropna())
        if schema_version[0] == "0":
            geometry_types_name = "geometry_type"
            if len(geometry_types) == 1:
                geometry_types = geometry_types[0]
        else:
            geometry_types_name = "geometry_types"

        crs = None
        if series.crs:
            if schema_version == "0.1.0":
                crs = series.crs.to_wkt()
            else:  # version >= 0.4.0
                crs = series.crs.to_json_dict()
                _remove_id_from_member_of_ensembles(crs)

        column_metadata[col] = {
            "encoding": "WKB",
            "crs": crs,
            geometry_types_name: geometry_types,
        }

        bbox = series.total_bounds.tolist()
        if np.isfinite(bbox).all():
            # don't add bbox with NaNs for empty / all-NA geometry column
            column_metadata[col]["bbox"] = bbox

    return {
        "primary_column": df._geometry_column_name,
        "columns": column_metadata,
        "version": schema_version or METADATA_VERSION,
        "creator": {"library": "geopandas", "version": geopandas.__version__},
    }


def _validate_dataframe(df):
    """Validate that the GeoDataFrame conforms to requirements for writing
    to Parquet format.

    Raises `ValueError` if the GeoDataFrame is not valid.

    copied from `pandas.io.parquet`

    Parameters
    ----------
    df : GeoDataFrame
    """

    if not isinstance(df, DataFrame):
        raise ValueError("Writing to Parquet/Feather only supports IO with DataFrames")

    # must have value column names (strings only)
    if df.columns.inferred_type not in {"string", "unicode", "empty"}:
        raise ValueError("Writing to Parquet/Feather requires string column names")

    # index level names must be strings
    valid_names = all(
        isinstance(name, str) for name in df.index.names if name is not None
    )
    if not valid_names:
        raise ValueError("Index level names must be strings")


def _geopandas_to_arrow(df, schema = None, index=None, schema_version=None):
    """
    Helper function with main, shared logic for to_parquet/to_feather.
    """
    from pyarrow import Table

    _validate_dataframe(df)

    # create geo metadata before altering incoming data frame
    geo_metadata = _create_metadata(df, schema_version=schema_version)

    if any(df[col].array.has_z.any() for col in df.columns[df.dtypes == "geometry"]):
        raise ValueError("Cannot write 3D geometries")
    df = df.to_wkb()

    table = Table.from_pandas(df, schema = schema, preserve_index=index)

    # Store geopandas specific file-level metadata
    # This must be done AFTER creating the table or it is not persisted
    metadata = table.schema.metadata
    metadata.update({b"geo": json.dumps(geo_metadata).encode("utf-8")})
    if schema:
        metadata.update(schema.metadata)

    return table.replace_schema_metadata(metadata)


def to_parquet(
    df, path, schema = None, index=None, compression="snappy", schema_version=None, **kwargs
):
    """
    Write a GeoDataFrame to the Parquet format.

    Any geometry columns present are serialized to WKB format in the file.

    Requires 'pyarrow'.

    This is tracking version 1.0.0 of the GeoParquet specification at:
    https://github.com/opengeospatial/geoparquet. Writing older versions is
    supported using the `schema_version` keyword.

    .. versionadded:: 0.8

    Parameters
    ----------
    path : str, path object
    index : bool, default None
        If ``True``, always include the dataframe's index(es) as columns
        in the file output.
        If ``False``, the index(es) will not be written to the file.
        If ``None``, the index(ex) will be included as columns in the file
        output except `RangeIndex` which is stored as metadata only.
    compression : {'snappy', 'gzip', 'brotli', None}, default 'snappy'
        Name of the compression to use. Use ``None`` for no compression.
    schema_version : {'0.1.0', '0.4.0', '1.0.0', None}
        GeoParquet specification version; if not provided will default to
        latest supported version.
    **kwargs
        Additional keyword arguments passed to pyarrow.parquet.write_table().
    """
    parquet = import_optional_dependency(
        "pyarrow.parquet", extra="pyarrow is required for Parquet support."
    )

    path = _expand_user(path)
    table = _geopandas_to_arrow(df, schema = schema, index=index, schema_version=schema_version)
    parquet.write_table(table, path, compression=compression, **kwargs)
