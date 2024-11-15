from fiboa_cli import improve, validate
from click.testing import CliRunner
from glob import glob


def test_improve(tmp_file):
    # merge files in directory
    files = glob("tests/data-files/merge/at.parquet")
    runner = CliRunner()
    args = files + ['-o', tmp_file.name, '-sz', '-g', '-e']
    result = runner.invoke(improve, args)
    assert result.exit_code == 0, result.output

    # Merged parquet file should be valid
    result = runner.invoke(validate, [tmp_file.name, '--data'])
    assert result.exit_code == 0, result.output
