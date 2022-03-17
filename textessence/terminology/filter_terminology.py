import os
import json
from hedgepig_logger import log
from textessence.core.lib import stopwords
from textessence.core.logic import getTerminologyWorkingEnvironment
from textessence.core.models.terminology_data_models import *

def filterTerminology(env, raw_terminology, stopword_options=None,
        categories=None, min_length=0, remove_stopword_terms=False):
    if categories:
        categories = set(categories)
        if not os.path.exists(env.terminology.category_map_file):
            raise Exception('Filtering settings included category-based filtering,'
                            ' but category map has not been extracted. Please extract'
                            ' category map before running terminology filtering.')
        category_map = CategoryMap(env.terminology.category_map_file)

        def category_filter(key, value):
            cats = category_map.get(key, set())
            if not (type(cats) is set):
                cats = set([cats])
            return len(cats.intersection(categories)) > 0
    else:
        category_filter = lambda key, value: True

    if min_length > 0:
        length_filter = lambda key, value: len(value) >= min_length
    else:
        length_filter = lambda key, value: True

    if remove_stopword_terms:
        stops = stopwords.buildStopwords(stopword_options)
        # stopword filtering currently implemented with whitespace-based
        # approximation of tokenization only
        def stopword_filter(key, value):
            value_tokens = value.split()
            valid = False
            for t in value_tokens:
                if not (t.lower() in stops.stopwords):
                    valid = True
            return valid
    else:
        stopword_filter = lambda key, value: True

    log.track(message='  >> Processed {0:,} terminology entries', writeInterval=100)
    filtered_terminology = FlatTerminology()
    for (key, terms) in raw_terminology.items():
        for term in terms:
            if (
                category_filter(key, term)
                and length_filter(key, term)
                and stopword_filter(key, term)
            ):
                filtered_terminology.addMapping(
                    key,
                    term
                )
            log.tick()
    log.flushTracker()
    return filtered_terminology


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

    env = getTerminologyWorkingEnvironment(
        options.config_f,
        options.terminology
    )

    input_file = env.terminology.raw_terminology_file
    output_file = env.terminology.filtered_terminology_file

    stopword_options = stopwords.loadConfiguration(env.base_config['Stopwords'])
    filtering_settings = json.loads(
        env.term_config[options.terminology]['FilteringSettings']
    )

    logfile = os.path.join(env.terminology.root_directory, '{0}.filter_terminology.log'.format(options.terminology))
    log.start(logfile)
    log.writeConfig([
        ('Base configuration file', options.config_f),
        ('Terminology configuration file', env.term_config_f),
        ('Target terminology', options.terminology),
        ('Input raw terminology file', input_file),
        ('Output filtered terminology file', output_file),
        ('Filtering settings', sorted(filtering_settings.items())),
    ], 'Terminology filtering')

    log.writeln('Reading terminology from %s...' % input_file)
    raw_terminology = FlatTerminology(input_file)
    log.writeln('Terminology contains {0:,} keys, {1:,} total mappings before filtering.\n'.format(
        len(raw_terminology), raw_terminology.num_terms
    ))

    log.writeln('Filtering terminology...')
    filtered_terminology = filterTerminology(
        env,
        raw_terminology,
        stopword_options=stopword_options,
        **filtering_settings
    )
    log.writeln('Terminology contains {0:,} keys, {1:,} total mappings after filtering.\n'.format(
        len(filtered_terminology), filtered_terminology.num_terms
    ))

    log.writeln('Writing filtered terminology to %s...' % output_file)
    filtered_terminology.filepath = output_file
    filtered_terminology.write()
    log.writeln('Done.\n')

    log.stop()
