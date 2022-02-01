import configparser
from hedgepig_logger import log
from . import initializeTerminologyEnvironment
from . import sources
from .terminology_data_models import *
from .snomed_ct import snomed_ct_interface

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

    env = initializeTerminologyEnvironment(
        options.config_f,
        options.terminology
    )
    if env.terminology is None:
        env.terminology = env.terminology_collection.addTerminology(options.terminology)

    logfile = os.path.join(env.terminology.root_directory, '{0}.extract_terminology.log'.format(options.terminology))

    source_release = env.term_config[options.terminology]['SourceRelease']
    source_config = env.term_config[source_release]
    term_config = env.term_config[options.terminology]

    log.start(logfile)
    log.writeConfig([
        ('Configuration file', options.config_f),
        ('Terminology configuration file', env.term_config_f),
        ('Terminology', options.terminology),
        ('Terminology configuration', list(term_config.items())),
        ('Source release configuration', list(source_config.items()))
    ], 'Terminology extraction')

    flat_terminology = FlatTerminology(env.terminology.raw_terminology_file)

    if sources.parse(term_config['SourceType']) == sources.SNOMED_CT:
        snomed_ct_interface.populateFromSnomedCT(
            flat_terminology,
            source_config
        )

    log.writeln('Writing flat terminology to %s...' % flat_terminology.filepath)
    flat_terminology.write()

    log.stop()
