from .BaseNormalizer import BaseNormalizer
from .SpacyNormalizer import SpacyNormalizer

class NormalizationMethods:
    Spacy = 'spacy'

    @staticmethod
    def parse(key):
        if key.strip().lower() == NormalizationMethods.Spacy.lower():
            return NormalizationMethods.Spacy
        else:
            raise ValueError('Unknown normalization method "{0}"'.format(key))

def buildNormalizer(options):
    if options.method == NormalizationMethods.Spacy:
        return SpacyNormalizer(options)
    else:
        raise KeyError('No normalizer configured for "{0}"'.format(options.method))

class NormalizationOptions:
    method = None
    lower = False
    strip_punctuation = False
    normalize_numbers = False
    normalize_urls = False
    method_settings = None

    def __init__(self, method=NormalizationMethods.Spacy, lower=False,
            strip_punctuation=False, normalize_numbers=False,
            normalize_urls=False, **method_settings):
        self.method = NormalizationMethods.parse(method)
        self.lower = lower
        self.strip_punctuation = strip_punctuation
        self.normalize_numbers = normalize_numbers 
        self.normalize_urls = normalize_urls
        self.method_settings = method_settings

    def asLabeledList(self):
        return [
            ('Normalization method', self.method),
            ('Lowercasing', self.lower),
            ('Stripping punctuation', self.strip_punctuation),
            ('Normalizing numbers', self.normalize_numbers),
            ('Normalizing URLs', self.normalize_urls),
            ('Method-specific settings', str(sorted(self.method_settings.items())))
        ]

def loadConfiguration(section):
    options = NormalizationOptions(
        method=(
            section['Method']
        ),
        lower=(
            section['Lowercase'].strip().lower() == 'true'
        ),
        strip_punctuation=(
            section['StripPunctuation'].strip().lower() == 'true'
        ),
        normalize_numbers=(
            section['NormalizeNumbers'].strip().lower() == 'true'
        ),
        normalize_urls=(
            section['NormalizeURLs'].strip().lower() == 'true'
        ),
        **{
            kvpair.split('=')[0]:kvpair.split('=')[1]
                for kvpair in section['MethodSettings'].split(',')
        }
    )
    return options

def filenameLabel(options):
    lbl = [options.method]
    if options.lower:
        lbl.append('lower')
    if options.strip_punctuation:
        lbl.append('nopunct')
    if options.normalize_numbers:
        lbl.append('normnumbers')
    if options.normalize_urls:
        lbl.append('normurls')
    return '_'.join(lbl)
