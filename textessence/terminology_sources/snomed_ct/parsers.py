import csv
from .models import *

class SnomedBaseParser:
    
    def __init__(self, fpath, language_codes=None):
        self.fpath = fpath

        if language_codes is None:
            self.language_codes = set(['en'])
        else:
            self.language_codes = language_codes

    def __enter__(self):
        self._stream = open(self.fpath, 'r')
        self._reader = csv.DictReader(
            self._stream,
            delimiter='\t'
        )
        return self

    def __exit__(self, type, value, traceback):
        self._stream.close()

    def __iter__(self):
        return self

class TextDefinitionParser(SnomedBaseParser):
    def __next__(self):
        definition = None
        while definition is None:
            record = next(self._reader)
            if (
                (record['languageCode'] in self.language_codes)
                and (int(record['active']) == 1)
            ):
                definition = Definition(
                    ID=record['id'],
                    concept_ID=record['conceptId'],
                    text=record['term']
                )
        if definition is None:
            raise StopIteration
        return definition

class DescriptionParser(SnomedBaseParser):
    def __next__(self):
        description = None
        while description is None:
            record = next(self._reader)
            if (
                (record['languageCode'] in self.language_codes)
                and (int(record['active']) == 1)
            ):
                description = Description(
                    ID=record['id'],
                    concept_ID=record['conceptId'],
                    term=record['term']
                )
        if description is None:
            raise StopIteration
        return description

class ConceptParser(SnomedBaseParser):
    def __next__(self):
        concept = None
        while concept is None:
            record = next(self._reader)
            if (
                int(record['active']) == 1
            ):
                concept = Concept(
                    ID=record['id']
                )
        if concept is None:
            raise StopIteration
        return concept
