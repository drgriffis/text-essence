import os
import configparser
from hedgepig_logger import log
from textessence.terminology_sources import terminology_sources_interface
from textessence.core.models.terminology_data_models import FlatTerminology
from textessence.core.logic.terminology import getTerminologyWorkingEnvironment

def extractTerminology(base_config_filepath, terminology_name):
    env = getTerminologyWorkingEnvironment(
        base_config_filepath,
        terminology_name
    )
    if env.terminology is None:
        env.terminology = env.terminology_collection.addTerminology(options.terminology)

    logfile = os.path.join(
        env.terminology.root_directory, '{0}.extract_terminology.log'.format(
            terminology_name
        )
    )

    configuration = terminology_sources_interface.TerminologySourceConfiguration.loadConfiguration(
        env
    )
    extractor = terminology_sources_interface.buildExtractor(
        configuration
    )

    log.start(logfile)
    log.writeConfig([
        ('Configuration file', base_config_filepath),
        ('Terminology configuration file', env.terminologies_config_f),
        ('Terminology', terminology_name),
        ('Terminology configuration', list(env.terminology_config.items())),
        *extractor.listConfigurationSettings()
    ], 'Terminology extraction')

    flat_terminology = FlatTerminology(
        env.terminology.raw_terminology_file
    )
    extractor.populateFlatTerminology(
        flat_terminology
    )

    log.writeln('Writing flat terminology to %s...' % flat_terminology.filepath)
    flat_terminology.write()

    log.stop()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-c', '--config', dest='config_f',
            help='(required) configuration file')
        parser.add_option('-t', '--terminology', dest='terminology',
            help='(required) section name for terminology in config-terminologies.ini')
        (options, args) = parser.parse_args()
        if not options.config_f:
            parser.print_help()
            parser.error('Must provide --config')
        if not options.terminology:
            parser.print_help()
            parser.error('Must provide --terminology')
        return options
    options = _cli()

    extractTerminology(
        options.config_f,
        options.terminology
    )
