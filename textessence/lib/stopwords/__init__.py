import json
from .BaseStopwords import BaseStopwords
from .SpacyStopwords import SpacyStopwords

class StopwordSources:
    Spacy = 'spacy'

    @staticmethod
    def parse(key): 
        if key.strip().lower() == StopwordSources.Spacy.lower():
            return StopwordSources.Spacy
        else:
            raise ValueError('Unknown stopword source "{0}"'.format(key))

def buildStopwords(options):
    if options.source == StopwordSources.Spacy:
        return SpacyStopwords(options)
    else:
        raise KeyError('No stopwords configured for "{0}"'.format(options.source))

class StopwordOptions:
    source = None
    source_settings = None

    def __init__(self, source=StopwordSources.Spacy, source_settings=None):
        self.source = StopwordSources.parse(source)
        self.source_settings = source_settings

def loadConfiguration(section):
    options = StopwordOptions(
        source=(
            section['Source']
        ),
        source_settings=json.loads(
            section['SourceSettings']
        )
    )
    return options
