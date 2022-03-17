class TerminologySourceExtractorInterface:
    def __init__(self, configuration):
        self.configuration = configuration

    def listConfigurationSettings(self):
        return [
            ('Terminology source release', self.configuration.source_release),
            ('Terminology source type', self.configuration.source_type),
            ('Terminology source configuration', list(self.configuration.source_release_config.items()))
        ]

    def populateFlatTerminology(self, flat_terminology):
        return NotImplemented()
