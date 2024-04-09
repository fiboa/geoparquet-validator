import json
import pyarrow.types as pat

from jsonschema.validators import Draft7Validator
from .const import PA_TYPE_CHECK
from .jsonschema import create_jsonschema
from .util import log as log_, load_datatypes, load_file, load_fiboa_schema, load_parquet_data, load_parquet_schema, merge_schemas
from .validate_data import validate_column

STAC_COLLECTION_SCHEMA = "http://schemas.stacspec.org/v1.0.0/collection-spec/json-schema/collection.json"

def log(text: str, status="info"):
    # Indent logs
    log_("  - " + str(text), status)


def validate(file, config):
    if file.endswith(".json") or file.endswith(".geojson"):
        return validate_geojson(file, config)
    else:
        return validate_parquet(file, config)


def validate_collection(collection, config):
    valid = True

    # Check fiboa version
    if "fiboa_version" not in collection:
        log("No fiboa_version found in collection metadata", "error")
        valid = False
    elif config.get("fiboa_version") is None:
        config["fiboa_version"] = collection["fiboa_version"]

    log("fiboa version: " + collection["fiboa_version"])

    if collection["fiboa_version"] != config["fiboa_version"]:
        log(f"fiboa versions differs: Collection is {collection['fiboa_version']} and requested specification version is {config['fiboa_version']}", "warning")

    # Check STAC Collection
    if not validate_colletion_schema(collection):
        valid = False

    # Check extensions
    extensions = {}
    if "fiboa_extensions" in collection:
        if not isinstance(collection["fiboa_extensions"], list):
            log("fiboa_extensions must be a list", "error")
            valid = False
        else:
            ext_map = config.get("extension_schemas", [])
            for ext in collection["fiboa_extensions"]:
                try:
                    if ext in ext_map:
                        path = ext_map[ext]
                        log(f"Redirecting {ext} to {path}", "info")
                    else:
                        path = ext
                    extensions[ext] = load_file(path)
                except Exception as e:
                    log(f"Extension {ext} can't be loaded: {e}", "error")
                    valid = False

    extension_info = ", ".join(collection["fiboa_extensions"]) or "none"
    log("fiboa extensions: " + extension_info)

    return valid, extensions


def validate_geojson(file, config):
    if not config.get("collection"):
        log("No collection specified", "error")
        return False

    collection = load_file(config.get("collection"))

    valid, extensions = validate_collection(collection, config)

    core_schema = load_fiboa_schema(config)

    datatypes = load_datatypes(config["fiboa_version"])

    schema = create_jsonschema(core_schema, datatypes)

    # Load extensions
    ext_errors = []
    for ext in extensions:
        try:
            uri = ext
            if ext in config["extension_schemas"]:
                uri = config["extension_schemas"][ext]
            ext_schema = load_file(uri)
            json_schema = create_jsonschema(ext_schema, datatypes)
            extensions[ext] = lambda obj: validate_json_schema(obj, json_schema)
        except Exception as error:
            extensions[ext] = None
            ext_errors.append(f"Failed to load extension {ext}: {str(error)}")

    for error in ext_errors:
        log(error, "error")

    # Validate
    try:
        data = load_file(file)
    except Exception as error:
        log(error, "error")
        return False

    if not isinstance(data, dict):
        log("Must be a JSON object", "error")
        return False

    if data["type"] == "Feature":
        features = [data]
    elif data["type"] == "FeatureCollection":
        features = data["features"]
    elif data["type"] == "Collection":
        # Skipping specific checks related to STAC Collection
        return None
    else:
        log("Must be a GeoJSON Feature or FeatureCollection", "error")
        return False

    if len(features) == 0:
        log("Must contain at least one Feature", "error")
        return False

    for index, feature in enumerate(features):
        errors = validate_json_schema(feature, schema)
        if len(errors) > 0:
            valid = False

        label = feature.get("id", f"index: {index}")

        if not valid:
            for error in errors:
                log(f"{label}: {error}", "error")
        else:
            for ext, validate_fn in extensions.items():
                if validate_fn:
                    ext_errors = validate_fn(feature)
                    if len(ext_errors) > 0:
                        for error in ext_errors:
                            log(f"{label} (ext {ext}): {error}", "error")
                        valid = False
                else:
                    log(f"{label}: Extension {ext} SKIPPED", "warning")
            if valid and len(features) > 1:
                log(f"{label}: VALID", "success")

    return valid


def validate_parquet(file, config):
    parquet_schema = load_parquet_schema(file)

    # Validate geo metadata in Parquet header
    if b"geo" not in parquet_schema.metadata:
        log("Parquet file schema does not have 'geo' key", "error")
        return False
    else:
        # ToDo: We are not checking whether this is a valid GeoParquet file
        log("The validator doesn't check whether this file contains valid GeoParquet metadata.", "info")

    # Validate fiboa metadata in Parquet header
    collection = {}
    if b"fiboa" not in parquet_schema.metadata:
        log("Parquet file schema does not have a 'fiboa' key", "warning")
        if not config.get("collection"):
            log("No collection specified", "error")
            return False
        else:
            collection = load_file(config.get("collection"))
    else:
        collection = json.loads(parquet_schema.metadata[b"fiboa"])

    # Validate Collection
    valid, extensions = validate_collection(collection, config)

    # load the actual fiboa schema
    fiboa_schema = load_fiboa_schema(config)

    # Load data if needed
    gdf = None
    if config.get("data"):
        try:
            gdf = load_parquet_data(file)
        except Exception as e:
            log(f"Data could not be read: {e}", "error")
            valid = False

    # Compile all properties from the schemas
    schemas = fiboa_schema
    for ext in extensions.values():
        schemas = merge_schemas(schemas, ext)

    # Check that all required fields are present
    for key in schemas.get("required", []):
        if key not in parquet_schema.names:
            log(f"{key}: Required field is missing", "error")
            valid = False

    # Validate whether the Parquet schema complies with the property schemas
    properties = schemas.get("properties", {})
    for key in parquet_schema.names:
        # Ignore fields without a schema
        if key not in properties:
            log(f"{key}: No schema defined")
            continue

        prop_schema = properties[key]
        # Make sure the schema has a data type assigned
        dtype = prop_schema.get("type", None)
        if dtype is None:
            log(f"{key}: No type specified", "warning")
            continue

        pq_field = parquet_schema.field(key)
        pq_type = pq_field.type

        # Does the field (dis)allow null?
        nullable = key not in schemas.get("required", [])
        if nullable != pq_field.nullable:
            log(f"{key}: Nullability differs, is {pq_field.nullable} but must be {nullable}", "error")
            valid = False

        # Is the data type of the field correct?
        pa_check = PA_TYPE_CHECK.get(dtype, None)
        if pa_check is None:
            log(f"{key}: Validating {dtype} is not supported yet", "warning")
            continue
        elif not pa_check(pq_type):
            log(f"{key}: Data type invalid, is {pq_type} but must be {dtype}", "error")
            valid = False
            continue

        # Check specifics of some types
        if dtype == "date-time":
            if pq_type.unit != "ms":
                log(f"{key}: Timestamp unit differs, should be ms", "warning")
            if pq_type.tz != "UTC":
                log(f"{key}: Timestamp timezone invalid, must be UTC", "error")
                valid = False
        elif dtype == "object":
            if not pat.is_string(pq_field.key_type):
                log(f"{key}: Map key datatype is not string", "error")
                valid = False

        # Validate data of the column
        if gdf is not None:
            issues = validate_column(gdf[key], prop_schema)
            if len(issues) > 0:
                for issue in issues:
                    log(f"{key}: {issue}")
                valid = False

    # Show a note once if data was not validated
    if not config.get("data"):
        log("Data was not validated as the --data parameter was not provided", "info")

    return valid


# todo: use stac_validator instead of our own validation routine
def validate_colletion_schema(obj):
    schema = load_file(STAC_COLLECTION_SCHEMA)
    errors = validate_json_schema(obj, schema)
    for error in errors:
        log(f"Collection: {error.path}: {error.message}", "error")

    return len(errors) == 0


def validate_json_schema(obj, schema):
    if isinstance(obj, (bytearray, bytes, str)):
        obj = json.loads(obj)

    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(obj), key=lambda e: e.path)
    return errors
