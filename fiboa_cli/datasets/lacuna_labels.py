from ..convert_utils import convert as convert_


SOURCES = "https://zenodo.org/records/11060871/files/mapped_fields_final.parquet?download=1"
ID = "lacuna"
SHORT_NAME = "Lacuna Labels"
TITLE = "A region-wide, multi-year set of crop field boundary labels for Africa"
DESCRIPTION = """
The [Lacunalabels](https://github.com/agroimpacts/lacunalabels/) repository hosts
the analytical code and pointers to datasets
resulting from a project to generate a continent-wide set of crop field
labels for Africa covering the years 2017-2023. The data are intended
for training and assessing machine learning models that can be used to
map agricultural fields over large areas and multiple years.

The project was funded by the [Lacuna Fund](https://lacunafund.org/),
and led by [Farmerline](https://farmerline.co/), in collaboration with
[Spatial Collective](https://spatialcollective.com/) and the
[Agricultural Impacts Research Group](agroimpacts.info) at
[Clark University](https://www.clarku.edu/departments/geography/).

Please refer to the [technical report](docs/report/technical-report.pdf)
for more details on the methods used to develop the dataset, an analysis
of label quality, and usage guidelines.

Data is published at https://zenodo.org/records/11060871 and can be used in accordance with
[Planet’s participant license agreement for the NICFI contract](https://go.planet.com/nicfi-pla-2024).
"""
PROVIDERS = [
    {
        "name": "Agricultural Impacts Research Group",
        "url": "https://github.com/agroimpacts/lacunalabels/",
        "roles": ["producer"]
    },
    {
        "name": "Planet Labs PBC",
        "url": "https://www.planet.com/",
        "roles": ["licensor"]
    }
]
ATTRIBUTION = "Planet Labs Inc."

LICENSE = {
    "title": "Planet NICFI participant license agreement",
    "href": "https://go.planet.com/nicfi-pla-2024",
    "type": "text/html",
    "rel": "license"
}

COLUMNS = {
    "geometry": "geometry",
    "name": "id",
    "category": "category",
}

MISSING_SCHEMAS = {
    "properties": {
        "category": {
            "type": "string"
        },
    }
}


def convert(output_file, cache = None, mapping_file=None, **kwargs):
    convert_(
        output_file,
        cache,
        SOURCES,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        providers=PROVIDERS,
        missing_schemas=MISSING_SCHEMAS,
        attribution=ATTRIBUTION,
        license=LICENSE,
        **kwargs
    )
