import importlib

def convert(dataset, output_file, cache_file = None, source_coop_url = None, collection = False):
    if dataset == "template":
        raise Exception(f"Converter for dataset 'template' not available")
    try:
        module_name = f".datasets.{dataset}"
        converter = importlib.import_module(module_name, package="fiboa_cli")
    except ImportError as e:
        raise Exception(f"Converter for '{dataset}' not available or faulty: {e}")

    converter.convert(output_file, cache_file = cache_file, source_coop_url = source_coop_url, collection = collection)
