import os
import sys
from hedgepig_logger import log
from . import initializeJETEnvironment

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-c', '--config', dest='config_f',
            help='(required) configuration file')
        parser.add_option('-t', '--terminology', dest='terminology',
            help='(required) section name for terminology in config-terminologies.ini')
        parser.add_option('-s', '--snapshot', dest='snapshot',
            help='(required) section name for target snapshot in config-snapshots.ini')
        parser.add_option('--threads', dest='num_threads',
            help='number of threads to use for tagging the corpus (default %default)',
            type='int', default=3)
        parser.add_option('--max-lines-in-queue', dest='max_lines_in_queue',
            help='maximum number of lines from the corpus to hold in memory at once'
                 ' (default %default)',
            type='int', default=1000)
        (options, args) = parser.parse_args()
        if not options.config_f:
            parser.print_help()
            parser.error('Must provide --config')
        if not options.terminology:
            parser.print_help()
            parser.error('Must provide --terminology')
        if not options.snapshot:
            parser.print_help()
            parser.error('Must provide --snapshot')
        return options
    options = _cli()

    env = initializeJETEnvironment(
        options.config_f,
        options.terminology,
        snapshot=options.snapshot
    )

    preprocessed_dir = env.terminology.preprocessed_dir(env.normalization_options)
    preprocessed_input_file = env.terminology.preprocessed_terminology_file(env.normalization_options)
    compiled_output_base = env.terminology.compiled_preprocessed_terminology_file(
        env.normalization_options,
        suffix=''
    )

    sys.path.append(env.JET_path)
    import JET.API
    
    input_filepath = env.snapshot_corpus.preprocessed_corpus_file(
        env.normalization_options
    )
    terminology_pkl_f = env.terminology.compiled_preprocessed_terminology_file(
        env.normalization_options,
        suffix='.ngram_term_map.pkl.gz'
    )
    output_filepath = env.snapshot_corpus.tagged_corpus_file(
        env.normalization_options,
        options.terminology
    )

    logfile = os.path.join(
        env.snapshot_root_dir,
        '{0}.tag_corpus.{1}.log'.format(
            options.snapshot,
            options.terminology
        )
    )
    log.start(logfile)
    log.writeConfig([
        ('Base configuration file', options.config_f),
        ('Terminology configuration file', env.term_config_f),
        ('Snapshots configuration file', env.snapshots_config_f),
        ('Target terminology', options.terminology),
        ('Target snapshot', options.snapshot),
        ('Input (preprocessed) corpus file', input_filepath),
        ('Compiled terminology file', terminology_pkl_f),
        ('Output annotations file', output_filepath),
        ('Normalization options (for filename calculation only)', env.normalization_options.asLabeledList()),
        ('Number of threads for tagging', options.num_threads),
        ('Queue size for tagging', options.max_lines_in_queue),
    ], 'JET corpus tagging')

    JET.API.tagCorpus(
        input_filepath,
        terminology_pkl_f,
        output_filepath,
        tokenizer=JET.API.tokenization.PreTokenized,
        num_threads=options.num_threads,
        max_lines_in_queue=options.max_lines_in_queue
    )

    log.stop()
