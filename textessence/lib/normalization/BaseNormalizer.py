class BaseNormalizer:
    '''All methods in this class must be overridden by each child class'''


    '''Primary wrapper method to run both tokenization and normalization.
    To be overridden in child classes.

    @input string
    @output iterable of lists of normalized tokens (one per sentence)
    '''
    def tokenizeAndNormalize(self, string):
        return NotImplemented()

    '''Tokenization only; for normal use, use tokenizeAndNormalize.
    To be overridden in child classes.

    @input string
    @output iterable of lists of tokenizer output objects (method-specific)
    '''
    def tokenize(self, string):
        return NotImplemented()

    '''Normalization only; for normal use, use tokenizeAndNormalize.
    To be overridden in child classes.

    @input single list of tokenizer output objects (i.e., one sentence; method-specific)
    @output list of strings (normalized tokens)
    '''
    def normalize(self, sent):
        return NotImplemented()
