import os
from hedgepig_logger import log
from . import initializeTerminologyEnvironment
from .terminology_data_models import *
from . import categories

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

    categories_config = categories.CategoriesConfiguration.loadConfiguration(
        env.term_config,
        options.terminology
    )

    logfile = os.path.join(env.terminology.root_directory, '{0}.map_terminology_categories.log'.format(options.terminology))
    log.start(logfile)
    log.writeConfig([
        ('Configuration file', options.config_f),
        ('Terminology configuration file', env.term_config_f),
        ('Terminology', options.terminology),
        ('Terminology configuration', list(env.term_config.items())),
        ('Categories source release configuration', list(categories_config.source_release_config.items())),
    ], 'Terminology category mapping')

    log.writeln('Building category map...')
    log.indent()
    category_map = categories.buildCategoryMap(categories_config)
    log.unindent()

    category_map.filepath = env.terminology.category_map_file
    log.writeln('Writing category map out to %s...' % category_map.filepath)
    category_map.write()
    log.writeln('Wrote {0:,} keys, {1:,} total mappings.\n'.format(
        len(category_map),
        category_map.num_mappings
    ))

    log.stop()
