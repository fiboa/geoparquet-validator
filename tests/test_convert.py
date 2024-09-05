import re
import tempfile
from pytest import fixture, mark
from fiboa_cli import convert, validate
from click.testing import CliRunner

"""
Create input files with: `ogr2ogr output.gpkg -limit 100 input.gpkg`
"""

@fixture
def out_file():
    with tempfile.NamedTemporaryFile() as out:
        yield out


@mark.parametrize('converter', ['at', 'be_vlg', 'br_ba_lem', 'de_sh', 'ec_lv', 'ec_si', 'fi', 'fr', 'hr', 'nl', 'nl_crop', 'pt', 'dk'])
def test_converter(out_file, converter):
    path = f"tests/data-files/convert/{converter}"
    runner = CliRunner()
    result = runner.invoke(convert, [converter, '-o', out_file.name, '-c', path])
    assert result.exit_code == 0, result.output
    error = re.search('Skipped - |No schema defined', result.output)
    if error:
        raise AssertionError(f"Found error in output: '{error.group(0)}'\n\n{result.output}")

    result = runner.invoke(validate, [out_file.name, '--data'])
    assert result.exit_code == 0, result.output


@mark.parametrize('args', [
    ['be_wa', ['tests/data-files/convert/be_wa/SIGEC_PARC_AGRI_ANON__2022_GEOPACKAGE_31370.zip|SIGEC_PARC_AGRI_ANON__2022.gpkg']],
    ['ai4sf', ['tests/data-files/convert/ai4sf/1_vietnam_areas.gpkg', 'tests/data-files/convert/ai4sf/4_cambodia_areas.gpkg']]
])
def test_converter_with_input(out_file, args):
    converter, input_files = args
    runner = CliRunner()
    args = [converter, '-o', out_file.name]
    for input_file in input_files:
        args.append('-i')
        args.append(input_file)
    result = runner.invoke(convert, args)
    assert result.exit_code == 0, result.output
    error = re.search('Skipped - |No schema defined', result.output)
    if error:
        raise AssertionError(f"Found error in output: '{error.group(0)}'\n\n{result.output}")

    result = runner.invoke(validate, [out_file.name, '--data'])
    assert result.exit_code == 0, result.output
