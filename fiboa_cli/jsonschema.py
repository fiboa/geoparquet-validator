import json
from .util import load_datatypes, load_fiboa_schema
from .jsonschema_template import jsonschema_template

def jsonschema(config):
    core_schema = load_fiboa_schema(config)
    datatypes = load_datatypes(config['fiboa_version'])
    schema = create_jsonschema(core_schema, datatypes, config.get('id'))
    if 'out' in config:
        with open(config['out'], 'w') as f:
            json.dump(schema, f, indent=2)
    else:
        print(schema)

def create_jsonschema(core_schema, datatypes, id=None):
    geojson_root_properties = ['id', 'geometry', 'bbox', 'properties']

    geojson = {
        'root': {
            'required': [],
            'properties': {}
        },
        'properties': {
            'required': [],
            'properties': {}
        }
    }

    for key, prop_schema in core_schema['properties'].items():
        result = convert_schema(prop_schema, datatypes)

        place = 'root' if key in geojson_root_properties else 'properties'
        if result['required']:
            geojson[place]['required'].append(key)
        geojson[place]['properties'][key] = result['schema']

    schema = jsonschema_template()
    if id:
        schema['$id'] = id
    else:
        schema.pop('$id', None)

    def merge(target, source):
        if 'required' in target and isinstance(target['required'], list):
            for prop in source['required']:
                if prop not in target['required']:
                    target['required'].append(prop)
        else:
            target['required'] = source['required']
        if 'properties' in target and isinstance(target['properties'], dict):
            target['properties'].update(source['properties'])
        else:
            target['properties'] = source['properties']

    merge(schema, geojson['root'])
    merge(schema['properties']['properties'], geojson['properties'])

    return schema

def convert_schema(prop_schema, datatypes):
    if not isinstance(prop_schema, dict) or 'type' not in prop_schema:
        return prop_schema
    elif prop_schema['type'] not in datatypes:
        raise ValueError(f"Unknown datatype {prop_schema['type']}")

    datatype_schema = datatypes[prop_schema['type']].copy()

    required = prop_schema.get('required')
    if required:
        if '$ref' in datatype_schema:
            datatype_schema = {
                "allOf": [
                    datatype_schema,
                    {
                        "not": {
                            "type": "null"
                        }
                    }
                ]
            }
    else:
        if isinstance(datatype_schema.get('type'), str):
            datatype_schema['type'] = [datatype_schema['type'], "null"]
        elif '$ref' in datatype_schema:
            datatype_schema = {
                "allOf": [
                    datatype_schema,
                    {
                        "type": "null"
                    }
                ]
            }
        elif (isinstance(datatype_schema.get('type'), list) or 
             isinstance(datatype_schema.get('oneOf'), list) or 
             isinstance(datatype_schema.get('anyOf'), list)):
            datatype_schema.get('type', []).append("null")
            datatype_schema.get('oneOf', []).append({"type": "null"})
            datatype_schema.get('anyOf', []).append({"type": "null"})
        else:
            print(f"Making schema {json.dumps(datatype_schema)} optional is not supported by this generator")

    # Avoid conflicting statements
    if 'exclusiveMaximum' in prop_schema:
        datatype_schema.pop('maximum', None)
    if 'exclusiveMinimum' in prop_schema:
        datatype_schema.pop('minimum', None)
    if 'maximum' in prop_schema:
        datatype_schema.pop('exclusiveMaximum', None)
    if 'minimum' in prop_schema:
        datatype_schema.pop('exclusiveMinimum', None)

    # Deep merge schemas
    for key, value in prop_schema.items():
        if key == 'items' and isinstance(value, dict):
            result = convert_schema(value, datatypes)
            datatype_schema['items'] = {**datatype_schema.get('items', {}), **result['schema']}
        elif key == 'properties' and isinstance(value, dict):
            if not isinstance(datatype_schema.get('properties'), dict):
                datatype_schema['properties'] = {}
            if not isinstance(datatype_schema.get('required'), list):
                datatype_schema['required'] = []
            for prop_name, prop_value in value.items():
                result = convert_schema(prop_value, datatypes)
                datatype_schema['properties'][prop_name] = {**datatype_schema['properties'].get(prop_name, {}), **result['schema']}
                if result['required']:
                    datatype_schema['required'].append(prop_name)
        elif key not in ['type', 'required']:
            datatype_schema[key] = value

    return {
        'required': required,
        'schema': datatype_schema
    }
