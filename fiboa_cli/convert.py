import importlib
import os

IGNORED_DATASET_FILES = ["__init__.py", "template.py"]

def convert(dataset, output_file, cache_file = None, source_coop_url = None, collection = False, compression = None):
    if dataset in IGNORED_DATASET_FILES:
        raise Exception(f"'{dataset}' is not a converter")
    try:
        converter = read_converter(dataset)
    except ImportError as e:
        raise Exception(f"Converter for '{dataset}' not available or faulty: {e}")

    converter.convert(output_file, cache_file = cache_file, source_coop_url = source_coop_url, collection = collection, compression = compression)

def list_all_converter_ids():
    datasets = importlib.import_module(".datasets", package="fiboa_cli")
    files = os.listdir(datasets.__path__[0])
    return [f[:-3] for f in files if f.endswith(".py") and f not in IGNORED_DATASET_FILES]

def list_all_converters(keys):
    converters = {}
    for id in list_all_converter_ids():
        obj = {}
        try:
            converter = read_converter(id)
            for key in keys:
                obj[key] = getattr(converter, key)
            converters[id] = obj
        except ImportError:
            pass
    return converters

def read_converter(id):
    module_name = f".datasets.{id}"
    return importlib.import_module(module_name, package="fiboa_cli")
