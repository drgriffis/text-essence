class TextCorpusSourceExtractorInterface:
    def __init__(self, sources_configuration, source_name):
        self.sources_configuration = sources_configuration
        self.source_name = source_name

    def listConfigurationSettings(self):
        return NotImplemented()

    def extractDocuments(self, collection):
        return NotImplemented()
