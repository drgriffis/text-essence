import scispacy
import spacy
from .BaseNormalizer import BaseNormalizer

class SpacyNormalizer(BaseNormalizer):
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

        model = options.method_settings.get('model', 'en_core_sci_lg')
        self.nlp = spacy.load(model)

    def tokenizeAndNormalize(self, string):
        for sent_tokens in self.tokenize(string):
            yield self.normalize(sent_tokens)

    def tokenize(self, string):
        para = self.nlp(string)
        for sent in para.sents:
            yield list(sent)

    def normalize(self, tokens):
        ## SpaCy object ops
        tokens = self.strip_punctuation_op(tokens)
        tokens = self.normalize_numbers_op(tokens)
        tokens = self.normalize_urls_op(tokens)
        ## switch to string ops
        tokens = [str(t) for t in tokens]
        tokens = self.lower_op(tokens)
        return tokens
