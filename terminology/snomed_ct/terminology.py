from .parsers import *
from hedgepig_logger import log

class SnomedTerminology:
    
    def __init__(self, concepts_file, descriptions_file, definitions_file, language_codes=None, verbose=False):
        concepts_by_ID = {}

        if verbose:
            status = lambda s: log.writeln('[SnomedTerminology] %s' % s)
        else:
            status = lambda s: None

        status('Loading Concepts...')
        with ConceptParser(concepts_file, language_codes=language_codes) as parser:
            for concept in parser:
                concepts_by_ID[concept.ID] = concept

            status('Loaded {0:,} concepts.'.format(len(concepts_by_ID)))

        status('Loading Descriptions...')
        ctr = 0

        with DescriptionParser(descriptions_file, language_codes=language_codes) as parser:
            for description in parser:
                if description.concept_ID in concepts_by_ID:
                    concepts_by_ID[description.concept_ID].descriptions.append(description)
                    ctr += 1

            status('Loaded {0:,} Descriptions.'.format(ctr))

        status('Loading Definitions...')
        ctr = 0

        with TextDefinitionParser(definitions_file, language_codes=language_codes) as parser:
            for definition in parser:
                if definition.concept_ID in concepts_by_ID:
                    concepts_by_ID[definition.concept_ID].definition = definition
                    ctr += 1
            status('Loaded {0:,} Definitions.'.format(ctr))

        self.concepts = list(concepts_by_ID.values())

    def __iter__(self):
        return iter(self.concepts)

    def __len__(self):
        return len(self.concepts)
