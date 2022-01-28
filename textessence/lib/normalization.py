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

class CLI:
    @staticmethod
    def addNormalizationOptions(parser):
        parser.add_option('--lower', dest='lower',
            action='store_true', default=False,
            help='lowercase all text')
        parser.add_option('--strip-punctuation', dest='strip_punctuation',
            action='store_true', default=False,
            help='strip punctuation tokens')
        parser.add_option('--normalize-numbers', dest='normalize_numbers',
            action='store_true', default=False,
            help='normalize digits to [NUMBER]')
        parser.add_option('--normalize-urls', dest='normalize_urls',
            action='store_true', default=False,
            help='normalize URLs to [URL]')
    @staticmethod
    def logNormalizationOptions(options):
        return [
            ('Lowercasing', options.lower),
            ('Stripping punctuation', options.strip_punctuation),
            ('Normalizing numbers', options.normalize_numbers),
            ('Normalizing URLs', options.normalize_urls),
        ]
