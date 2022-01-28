import configparser
from .terminology import SnomedTerminology
from hedgepig_logger import log

def populateFromSnomedCT(flat_terminology, source_config):
    log.writeln('Loading SNOMED-CT terminology...')
    snomed_terminology = SnomedTerminology(
        concepts_file=source_config['ConceptsFile'],
        descriptions_file=source_config['DescriptionsFile'],
        definitions_file=source_config['DefinitionsFile'],
        verbose=True
    )
    log.writeln('Loaded {0:,} concepts.\n'.format(len(snomed_terminology)))

    for concept in snomed_terminology:
        for description in concept.descriptions:
            flat_terminology.addMapping(
                concept.ID,
                description.term
            )
