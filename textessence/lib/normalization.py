class NormalizationOptions:
    lower = False
    strip_punctuation = False
    normalize_numbers = False
    normalize_urls = False

    def __init__(self, lower=False, strip_punctuation=False, normalize_numbers=False, normalize_urls=False):
        self.lower = lower
        self.strip_punctuation = strip_punctuation
        self.normalize_numbers = normalize_numbers 
        self.normalize_urls = normalize_urls

    def asLabeledList(self):
        return [
            ('Lowercasing', self.lower),
            ('Stripping punctuation', self.strip_punctuation),
            ('Normalizing numbers', self.normalize_numbers),
            ('Normalizing URLs', self.normalize_urls),
        ]

class Normalizer:
    def __init__(self, options):
        if options.strip_punctuation:
            self.strip_punctuation_op = lambda tokens: [
                t
                    for t in tokens
                    if not t.is_punct
            ]
        else:
            self.strip_punctuation_op = lambda tokens: tokens

        if options.normalize_numbers:
            self.normalize_numbers_op = lambda tokens: [
                ('[NUMBER]' if t.like_num else t)  # t.is_digit is only True if it is an integer
                    for t in tokens
            ]
        else:
            self.normalize_numbers_op = lambda tokens: tokens

        if options.normalize_urls:
            self.normalize_urls_op = lambda tokens: [
                (
                    t 
                    if (
                        (type(t) is str)           # numbers may already be strings
                        or (not t.like_url)
                    )
                    else '[URL]'
                )
                    for t in tokens
            ]
        else:
            self.normalize_urls_op = lambda tokens: tokens

        if options.lower:
            self.lower_op = lambda tokens: [
                t.lower()
                    for t in tokens
            ]
        else:
            self.lower_op = lambda tokens: tokens

    def normalize(self, sent):
        tokens = list(sent)
        ## SpaCy object ops
        tokens = self.strip_punctuation_op(tokens)
        tokens = self.normalize_numbers_op(tokens)
        tokens = self.normalize_urls_op(tokens)
        ## switch to string ops
        tokens = [str(t) for t in tokens]
        tokens = self.lower_op(tokens)
        return tokens

def loadConfiguration(section):
    options = NormalizationOptions(
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
        )
    )
    return options

def filenameLabel(options):
    lbl = []
    if options.lower:
        lbl.append('lower')
    if options.strip_punctuation:
        lbl.append('nopunct')
    if options.normalize_numbers:
        lbl.append('normnumbers')
    if options.normalize_urls:
        lbl.append('normurls')
    return '_'.join(lbl)
