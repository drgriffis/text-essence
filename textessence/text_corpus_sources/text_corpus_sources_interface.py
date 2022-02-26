import json
from hedgepig_logger import log
from . import cord19

class TextCorpusSources:
    CORD19_Diachronic = 'CORD-19 Diachronic'

    @staticmethod
    def parse(value):
        if value.strip().lower() == TextCorpusSources.CORD19_Diachronic.lower():
            return TextCorpusSources.CORD19_Diachronic
        else:
            raise ValueError('Text corpus source "{0}" is not yet configured.'.format(value))

class TextCorpusSourceConfiguration:
    sources_configuration = None
    source_name = None
    source_type = None
    source_settings = None

    def __init__(self, sources_configuration, source_name, source_type, source_settings):
        self.sources_configuration = sources_configuration
        self.source_name = source_name
        self.source_type = source_type
        self.source_settings = source_settings

    @staticmethod
    def loadConfiguration(sources_config, source_name):
        source_settings = sources_config[source_name]
        return TextCorpusSourceConfiguration(
            sources_configuration=sources_config,
            source_name=source_name,
            source_type=TextCorpusSources.parse(
                source_settings['SourceType']
            ),
            source_settings=source_settings
        )

def buildExtractor(configuration):
    if configuration.source_type == TextCorpusSources.CORD19_Diachronic:
        return cord19.cord19_article_extraction.CORD19DumpExtractor(
            configuration.sources_configuration,
            configuration.source_name
        )
    else:
        raise KeyError('Text corpus source "{0}" is not configured.'.format(configuration.source_type))
