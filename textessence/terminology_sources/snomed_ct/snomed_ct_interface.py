import configparser
from .terminology import SnomedTerminology
from hedgepig_logger import log
from ..terminology_source_extractor_interface import TerminologySourceExtractorInterface

class SnomedCtExtractor(TerminologySourceExtractorInterface):
    def populateFlatTerminology(self, flat_terminology):
        log.writeln('Loading SNOMED-CT terminology...')
        snomed_terminology = SnomedTerminology(
            concepts_file=self.configuration.source_release_config['ConceptsFile'],
            descriptions_file=self.configuration.source_release_config['DescriptionsFile'],
            definitions_file=self.configuration.source_release_config['DefinitionsFile'],
            verbose=True
        )
        log.writeln('Loaded {0:,} concepts.\n'.format(len(snomed_terminology)))

        for concept in snomed_terminology:
            for description in concept.descriptions:
                flat_terminology.addMapping(
                    concept.ID,
                    description.term
                )
