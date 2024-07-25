from ..convert_utils import convert as convert_

SOURCES = {
    "https://phys-techsciences.datastations.nl/api/access/datafile/100634?gbrecs=true": "2_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100282?gbrecs=true": "3_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100392?gbrecs=true": "4_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100252?gbrecs=true": "5_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100072?gbrecs=true": "6_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100169?gbrecs=true": "7_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100348?gbrecs=true": "8_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100487?gbrecs=true": "9_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100084?gbrecs=true": "10_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100155?gbrecs=true": "11_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100475?gbrecs=true": "12_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100372?gbrecs=true": "13_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100196?gbrecs=true": "14_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100006?gbrecs=true": "15_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100248?gbrecs=true": "16_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100397?gbrecs=true": "17_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100217?gbrecs=true": "18_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100652?gbrecs=true": "19_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100326?gbrecs=true": "20_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100625?gbrecs=true": "21_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100413?gbrecs=true": "33_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100593?gbrecs=true": "34_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100057?gbrecs=true": "35_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100536?gbrecs=true": "36_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100343?gbrecs=true": "37_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100711?gbrecs=true": "38_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100313?gbrecs=true": "39_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100679?gbrecs=true": "57_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100191?gbrecs=true": "58_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100023?gbrecs=true": "59_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100025?gbrecs=true": "60_cambodia_areas.gpkg",
    "https://phys-techsciences.datastations.nl/api/access/datafile/100543?gbrecs=true": "61_cambodia_areas.gpkg",
}

ID = "ai4sf_kh"
SHORT_NAME = "Cambodia (AI4SmallFarms)"
TITLE = "Field boundaries for Cambodia"
# from https://research.tudelft.nl/en/publications/ai4smallfarms-a-dataset-for-crop-field-delineation-in-southeast-a
DESCRIPTION = """
Agricultural field polygons within smallholder farming systems are essential to facilitate the collection of geo-spatial data useful for farmers, managers, and policymakers.
However, the limited availability of training labels poses a challenge in developing supervised methods to accurately delineate field boundaries using Earth Observation (EO) data.
This data set allows researchers to test and benchmark machine learning methods to delineate agricultural field boundaries in polygon format.
The large-scale data set consists of 439,001 field polygons divided into 62 tiles of approximately 5×5 km distributed across Vietnam and Cambodia, covering a range of fields and diverse landscape types.
The field polygons have been meticulously digitized from satellite images, following a rigorous multi-step quality control process and topological consistency checks.
Multi-temporal composites of Sentinel-2 (S2) images are provided to ensure cloud-free data.
"""

PROVIDERS = [
    {
        "name": "DATA Archiving and Networked Services (DANS)",
        "url": "https://research.tudelft.nl/en/publications/ai4smallfarms-a-dataset-for-crop-field-delineation-in-southeast-a",
        "roles": ["producer", "licensor"]
    }
]
ATTRIBUTION = "Persello, C., Grift, J., Fan, X., Paris, C., Hansch, R., Koeva, M., & Nelson, A. (2023). AI4SmallFarms: A Dataset for Crop Field Delineation in Southeast Asian Smallholder Farms. IEEE Geoscience and Remote Sensing Letters, 20, 1-5. Article 2505705. https://doi.org/10.1109/LGRS.2023.3323095"
LICENSE = "CC-BY-4.0"

COLUMNS = {
    'fiboa_id': 'id',
    'id' : 'group',
    '_predicate' : '_predicate',
    'country': 'country',
    'geometry' : 'geometry',
}

ADD_COLUMNS = {
    "determination_datetime": "2021-08-01T00:00:00Z",
    "determination_method": "auto-imagery"
}

def migrate(gdf):
    gdf['fiboa_id'] = gdf['id'].astype(str) + "_" + gdf.index.astype(str)
    return gdf

MIGRATION = migrate

MISSING_SCHEMAS = {
    "properties": {
        "group": {
            "type": "uint8"
        },
        "group_id": {
            "type": "uint16"
        },
        "_predicate": {
            "type": "string",
            "enum": ["INTERSECTS"]
        },
        "country": {
            "type": "string",
            "enum": ["cambodia"]
        }
    }
}


# Conversion function, usually no changes required
def convert(output_file, input_files = None, cache = None, source_coop_url = None, collection = False, compression = None):
    convert_(
        output_file,
        cache,
        SOURCES,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        input_files=input_files,
        providers=PROVIDERS,
        source_coop_url=source_coop_url,
        missing_schemas=MISSING_SCHEMAS,
        column_additions=ADD_COLUMNS,
        migration=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )
