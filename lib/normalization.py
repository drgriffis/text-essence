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

        if options.normalize_digits:
            self.normalize_digits_op = lambda tokens: [
                ('[DIGITS]' if t.is_digit else t)
                    for t in tokens
            ]
        else:
            self.normalize_digits_op = lambda tokens: tokens

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
        tokens = self.normalize_digits_op(tokens)
        ## switch to string ops
        tokens = [str(t) for t in tokens]
        tokens = self.lower_op(tokens)
        return tokens

class CLI:
    @staticmethod
    def addNormalizationOptions(parser):
        parser.add_option('--lower', dest='lower',
            action='store_true', default=False,
            help='lowercase all text')
        parser.add_option('--strip-punctuation', dest='strip_punctuation',
            action='store_true', default=False,
            help='strip punctuation tokens')
        parser.add_option('--normalize-digits', dest='normalize_digits',
            action='store_true', default=False,
            help='normalize digits to [DIGITS]')
    @staticmethod
    def logNormalizationOptions(options):
        return [
            ('Lowercasing', options.lower),
            ('Stripping punctuation', options.strip_punctuation),
            ('Normalizing digits', options.normalize_digits),
        ]
