import scispacy
import spacy
from .BaseStopwords import BaseStopwords

class SpacyStopwords(BaseStopwords):
    def __init__(self, options):
        model = options.source_settings.get('model', 'en_core_sci_lg')
        nlp = spacy.load(model)
        self.stopwords = set(nlp.Defaults.stop_words)
