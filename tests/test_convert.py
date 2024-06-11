import re
from pytest import fixture, mark
from fiboa_cli.util import load_file
from fiboa_cli import convert, validate
import tempfile
from click.testing import CliRunner

"""
Create input files with: `ogr2ogr output.gpkg -limit 100 input.gpkg`
"""


@fixture
def out_file():
    with tempfile.NamedTemporaryFile() as out:
        yield out


@mark.parametrize("converter", ["nl", "nl_crop"])
def test_converter(out_file, converter):
    path = f"tests/data-files/{converter}.gpkg"
    assert load_file(path), f"Input file missing: {path}"

    runner = CliRunner()
    result = runner.invoke(convert, [converter, '-o', out_file.name, '-c', path])
    assert result.exit_code == 0, result.output
    error = re.search("Skipped - |No schema defined", result.output)
    if error:
        raise AssertionError(f"Found error in output: '{error.group(0)}'\n\n{result.output}")

    result = runner.invoke(validate, [out_file.name, '--data'])
    assert result.exit_code == 0, result.output
