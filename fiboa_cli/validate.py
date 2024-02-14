import json
import pyarrow.types as pat

from jsonschema.validators import Draft7Validator
from .const import PA_TYPE_CHECK
from .util import log as log_, load_file, load_fiboa_schema, load_parquet_schema

def log(text: str, status="info"):
  # Indent logs
  log_("  " + text, status)

def validate_schema(obj, schema, name):
  if isinstance(obj, str) or isinstance(obj, bytes) or isinstance(obj, bytearray):
    obj = json.loads(obj)

  errors = Draft7Validator(schema).iter_errors(obj)

  for error in errors:
    log(f"{name}: {error.json_path}: {error.message}", "warning")
    if "description" in error.schema:
      log(f"    \"{error.schema['description']}\"", "warning")

  return len(errors) > 0


def validate(file, config):
  valid = True
  parquet_schema = load_parquet_schema(file)

  # Validate geo metadata in Parquet header
  if b"geo" not in parquet_schema.metadata:
    log("Parquet file schema does not have 'geo' key", "error")
    return False
  else:
    # ToDo: We are not checking whether this is a valid GeoParquet file
    # We just check whether a geo key is present at all.
    # use gpq validate geoparquet metadata for now
    log(f"This validator doesn't check whether this file contains valid GeoParquet metadata, use gpq in addition for now.", "warning")
  
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

  # Check fiboa version
  if "fiboa_version" not in collection:
    log("No fiboa_version found in collection metadata", "error")
    valid = False
  elif config.get("fiboa_version") is None:
    config["fiboa_version"] = collection["fiboa_version"]

  # Check extensions
  extensions = {}
  if "fiboa_extensions" in collection:
    if not isinstance(collection["fiboa_extensions"], list):
      log("fiboa_extensions must be a list", "error")
      valid = False
    else:
      for ext in collection["fiboa_extensions"]:
        try:
          extensions[ext] = load_file(ext)
        except Exception as e:
          log(f"Extension {ext} can't be loaded: {e}", "error")
          valid = False

  # load the actual fiboa schema
  fiboa_schema = load_fiboa_schema(config)

  # Compile all properties from the schemas
  properties = fiboa_schema["properties"]
  for ext in extensions.values():
    properties.update(ext["properties"])

  # Validate whether the Parquet schema complies with the property schemas
  for key in parquet_schema.names:
    # Ignore fields without a schema
    if key not in properties:
      log(f"{key}: No schema defined")
      continue

    prop_schema = properties[key]
    # Make sure the schema has a data type assigned
    dtype = prop_schema.get("type", None)
    if (dtype is None):
      log(f"{key}: No type specified", "warning")
      continue

    pq_field = parquet_schema.field(key)
    pq_type = pq_field.type
    
    # Does the field (dis)allow null?
    nullable = not prop_schema.get("required", False)
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

  # validate_schema(parquet_schema.metadata[b"fiboa"], fiboa_schema, "fiboa")

  return valid
