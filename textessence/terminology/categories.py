import json
from hedgepig_logger import log
from .umls import umls_interface

class CategoriesSources:
    UMLS = 'UMLS'

    @staticmethod
    def parse(value):
        if value.strip().lower() == CategoriesSources.UMLS.lower():
            return CategoriesSources.UMLS
        else:
            raise ValueError('Semantic type source "{0}" is not yet configured.'.format(value))

class CategoriesConfiguration:
    source_type = None
    source_release = None
    source_mapping_settings = None
    source_release_config = None

    def __init__(self, source_type, source_release, source_mapping_settings, source_release_config):
        self.source_type = source_type
        self.source_release = source_release
        self.source_mapping_settings = source_mapping_settings
        self.source_release_config = source_release_config

    @staticmethod
    def loadConfiguration(term_config, terminology):
        source_release=term_config[terminology]['CategoriesSourceRelease']
        return CategoriesConfiguration(
            source_type=CategoriesSources.parse(
                term_config[terminology]['CategoriesSourceType']
            ),
            source_release=source_release,
            source_mapping_settings=json.loads(
                term_config[terminology]['CategoriesSourceMappingSettings']
            ),
            source_release_config=term_config[source_release]
        )



def buildCategoryMap(configuration):
    if configuration.source_type == CategoriesSources.UMLS:
        category_map = umls_interface.buildSemanticTypeMap(
            **configuration.source_release_config,
            **configuration.source_mapping_settings
        )
    else:
        raise KeyError('Categories source type "{0}" is not configured'.format(configuration.source_type))

    return category_map
