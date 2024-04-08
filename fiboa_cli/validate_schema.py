from jsonschema.validators import Draft202012Validator
from .util import log as log_, load_file

STAC_COLLECTION_SCHEMA = "http://schemas.stacspec.org/v1.0.0/collection-spec/json-schema/collection.json"

def log(text: str, status="info"):
    # Indent logs
    log_("  - " + str(text), status)


def validate_schema(file, config):
    schema = load_file(file)
    if not isinstance(schema, dict):
        log("Schema must be an object", "error")
        return False

    metaschema_uri = config.get("metaschema", None)
    if "schema" in schema:
        metaschema_uri = schema.get("schema", metaschema_uri)
    if metaschema_uri is None:
        log("No metaschema provided", "error")
        return False

    log(f"Metaschema: {metaschema_uri}", "info")

    metaschema = load_file(metaschema_uri)

    errors = validate_json_schema(schema, metaschema)
    if len(errors) > 0:
        for error in errors:
            log(error, "error")
        return False
    else:
        log("VALID", "success")
        return True

def validate_json_schema(obj, schema):
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(obj), key=lambda e: e.path)
    return errors
