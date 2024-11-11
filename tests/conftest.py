import tempfile
from pytest import fixture


@fixture
def tmp_file():
    with tempfile.NamedTemporaryFile() as out:
        yield out
