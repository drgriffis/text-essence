import os
import configparser
from hedgepig_logger import log
from textessence.core.models.snapshot_data_models import LiteratureSnapshotCollection
from textessence.text_corpus_sources import text_corpus_sources_interface

def extractDocumentsFromTextSource(base_config_filepath, source_name):
    base_config = configparser.ConfigParser()
    base_config.read(options.config_f)

    sources_config = configparser.ConfigParser()
    sources_config.read(base_config['General']['TextCorpusSourcesConfig'])

    logfile = os.path.join(
        base_config['Logging']['CORDExtractionLogs'],
        '{0}_extraction.log'.format(source_name)
    )
    output_root_dir = sources_config['Default']['ExtractionRootDirectory']

    configuration = text_corpus_sources_interface.TextCorpusSourceConfiguration.loadConfiguration(
        sources_config,
        source_name
    )
    extractor = text_corpus_sources_interface.buildExtractor(
        configuration
    )

    log.start(logfile)
    log.writeConfig([
        *extractor.listConfigurationSettings(),
        ('Output root directory', output_root_dir),
    ], 'Extracting CORD-19 corpus')

    collection = LiteratureSnapshotCollection(
        output_root_dir
    )

    extractor.extractDocuments(
        collection
    )

    log.stop()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-c', '--config', dest='config_f',
            help='(required) configuration file')
        parser.add_option('-s', '--source', dest='source_name',
            help='(required) name of text source to extract from')
        (options, args) = parser.parse_args()
        if not options.config_f:
            parser.print_help()
            parser.error('Must provide --config')
        if not options.source_name:
            parser.print_help()
            parser.error('Must provide --source')
        return options
    options = _cli()

    extractDocumentsFromTextSource(
        options.config_f,
        options.source_name
    )
