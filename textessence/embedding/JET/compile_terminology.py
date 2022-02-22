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
        (options, args) = parser.parse_args()
        if not options.config_f:
            parser.print_help()
            parser.error('Must provide --config')
        if not options.terminology:
            parser.print_help()
            parser.error('Must provide --terminology')
        return options
    options = _cli()

    env = initializeJETEnvironment(
        options.config_f,
        options.terminology
    )

    preprocessed_dir = env.terminology.preprocessed_dir(env.normalization_options)
    preprocessed_input_file = env.terminology.preprocessed_terminology_file(env.normalization_options)
    compiled_output_base = env.terminology.compiled_preprocessed_terminology_file(
        env.normalization_options,
        suffix=''
    )

    sys.path.append(env.JET_path)
    import JET.API

    logfile = os.path.join(preprocessed_dir, '{0}.compile_terminology.log'.format(options.terminology))
    log.start(logfile)
    log.writeConfig([
        ('Base configuration file', options.config_f),
        ('Terminology configuration file', env.term_config_f),
        ('Target terminology', options.terminology),
        ('Input (normalized) terminology file', preprocessed_input_file),
        ('Output (compiled) terminology base path', compiled_output_base),
        ('Normalization options (for filename calculation only)', env.normalization_options.asLabeledList())
    ], 'Terminology compilation')

    JET.API.compileTerminology(
        input_filepath=preprocessed_input_file,
        output_basepath=compiled_output_base,
        tokenizer=JET.API.tokenization.PreTokenized,
        remove_stopwords=False,
        use_collapsed_string=False,
        verbose=False,
        recursion_limit=1000
    )

    log.stop()
