import importlib
import os
from . import datasets

def convert(dataset, output_file, cache_file = None, source_coop_url = None, collection = False):
    if dataset == "template":
        raise Exception(f"Converter for dataset 'template' not available")
    try:
        converter = read_converter(dataset)
    except ImportError as e:
        raise Exception(f"Converter for '{dataset}' not available or faulty: {e}")

    converter.convert(output_file, cache_file = cache_file, source_coop_url = source_coop_url, collection = collection)

def list_all_converter_ids():
    files = os.listdir(datasets.__path__[0])
    ignore = ["__init__.py", "template.py"]
    return [f[:-3] for f in files if f.endswith(".py") and f not in ignore]

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
