from .snomed_ct import snomed_ct_interface

class TerminologySources:
    SNOMED_CT = 'SNOMED_CT'

    @staticmethod
    def parse(value):
        if value.strip().lower() == TerminologySources.SNOMED_CT.lower():
            return TerminologySources.SNOMED_CT
        else:
            raise ValueError('Terminology source "{0}" is not yet configured.'.format(value))

class TerminologySourceConfiguration:
    source_release = None
    source_type = None
    source_release_config = None

    def __init__(self, source_release, source_type, source_release_config):
        self.source_release = source_release
        self.source_type = source_type
        self.source_release_config = source_release_config

    @staticmethod
    def loadConfiguration(env):
        source_release = env.terminology_config['SourceRelease']
        source_release_config = env.terminologies_config[source_release]
        return TerminologySourceConfiguration(
            source_release=source_release,
            source_type=TerminologySources.parse(
                env.terminology_config['SourceType']
            ),
            source_release_config=source_release_config
        )

def buildExtractor(configuration):
    if configuration.source_type == TerminologySources.SNOMED_CT:
        return snomed_ct_interface.SnomedCtExtractor(
            configuration
        )
    else:
        raise KeyError('Terminology source "{0}" is not configured.'.format(configuration.source_type))
