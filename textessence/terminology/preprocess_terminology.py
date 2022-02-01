import os
from hedgepig_logger import log
from textessence.lib import normalization
from . import initializeTerminologyEnvironment
from .terminology_data_models import *

def normalizeTerminology(terminology, normalization_options):
    log.track(message='  >> Processed {0:,} terminology entries', writeInterval=100)
    normalizer = normalization.buildNormalizer(normalization_options)
    normalized_terminology = FlatTerminology()
    for (key, terms) in terminology.items():
        for term in terms:
            try:
                normalized_term = ' '.join([
                    ' '.join(sent_tokens)
                        for sent_tokens in normalizer.tokenizeAndNormalize(term)
                ])
                normalized_terminology.addMapping(
                    key,
                    normalized_term
                )
            except ValueError:
                log.writeln('\n[WARNING] Failed to parse line "%s"' % line)
            log.tick()
    log.flushTracker()
    return normalized_terminology

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

    normalization_options = normalization.loadConfiguration(env.base_config['Normalization'])
    preprocessed_dir = env.terminology.preprocessed_dir(normalization_options)

    input_file = env.terminology.raw_terminology_file
    preprocessed_output_file = env.terminology.preprocessed_terminology_file(normalization_options)

    if not os.path.exists(preprocessed_dir):
        log.writeln('Output directory {0} does not exist, attempting to create it...\n'.format(preprocessed_dir))
        os.mkdir(preprocessed_dir)

    logfile = os.path.join(preprocessed_dir, '{0}.preprocess_terminology.log'.format(options.terminology))
    log.start(logfile)
    log.writeConfig([
        ('Base configuration file', options.config_f),
        ('Terminology configuration file', env.term_config_f),
        ('Target terminology', options.terminology),
        ('Input (unnormalized) terminology file', input_file),
        ('Output (normalized) terminology file', preprocessed_output_file),
        ('Normalization options', normalization_options.asLabeledList())
    ], 'Terminology preprocessing')

    log.writeln('Reading terminology from %s...' % input_file)
    terminology = FlatTerminology(input_file)
    log.writeln('Terminology contains {0:,} keys, {1:,} total mappings before normalization.\n'.format(
        len(terminology), terminology.num_terms
    ))

    log.writeln('Normalizing terminology...')
    normalized_terminology = normalizeTerminology(
        terminology,
        normalization_options
    )
    log.writeln('Terminology contains {0:,} keys, {1:,} total mappings after normalization.\n'.format(
        len(normalized_terminology), normalized_terminology.num_terms
    ))

    log.writeln('Writing normalized terminology to %s...' % preprocessed_output_file)
    normalized_terminology.filepath = preprocessed_output_file
    normalized_terminology.write()
    log.writeln('Done.\n')

    log.stop()
