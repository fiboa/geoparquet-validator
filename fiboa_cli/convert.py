import importlib

def convert(dataset, output_file):
    if dataset == "template":
        raise Exception(f"Converter for dataset 'template' not available")
    try:
        module_name = f".datasets.{dataset}"
        converter = importlib.import_module(module_name, package="fiboa_cli")
    except ImportError as e:
        raise Exception(f"Converter for '{dataset}' not available or faulty: {e}")

    converter.convert(output_file)
