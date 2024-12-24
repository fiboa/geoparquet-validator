from fiboa_cli.convert_utils import BaseConverter
from fiboa_cli.datasets.commons.data import read_data_csv


class ESBaseConverter(BaseConverter):
    use_code_attribute = "uso_sigpac"

    extensions = [
        "https://fiboa.github.io/crop-extension/v0.1.0/schema.yaml",
        "https://fiboa.github.io/administrative-division-extension/v0.1.0/schema.yaml"
    ]
    column_additions = {
        # https://www.euskadi.eus/contenidos/informacion/pac2015_pagosdirectos/es_def/adjuntos/Anexos_PAC_marzo2015.pdf
        # https://www.fega.gob.es/sites/default/files/files/document/AD-CIRCULAR_2-2021_EE98293_SIGC2021.PDF
        # Very generic list
        "admin:country_code": "ES",
        "crop:code_list": "https://github.com/fiboa/cli/blob/main/fiboa_cli/datasets/data-files/es_coda_uso.csv",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert self.id.startswith("es_"), "Assuming Spanish subclass"

        def code_filter(col):
            return ~col.isin("AG/CA/ED/FO/IM/IS/IV/TH/ZC/ZU/ZV".split("/"))

        self.column_filters = {self.use_code_attribute: code_filter}
        self.column_additions["admin:subdivision_code"] = self.id[len("es_"):].upper()

    def migrate(self, gdf):
        # This actually is a land use code. Not sure if we should put this in crop:code
        rows = read_data_csv("es_coda_uso.csv")
        mapping = {row["original_code"]: row["original_name"] for row in rows}
        mapping_en = {row["original_code"]: row["name_en"] for row in rows}
        gdf['crop:name'] = gdf[self.use_code_attribute].map(mapping)
        gdf['crop:name_en'] = gdf[self.use_code_attribute].map(mapping_en)
        return gdf
