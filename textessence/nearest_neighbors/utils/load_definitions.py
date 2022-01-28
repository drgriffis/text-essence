import csv
import configparser
from hedgepig_logger import log
from ..data_models import *
from ..database import EmbeddingNeighborhoodDatabase

def readDefinitions(f, delimiter=','):
    definitions = {}
    with open(f, 'r') as stream:
        reader = csv.reader(stream, delimiter=delimiter)
        for record in reader:
            (entity_ID, definition) = record
            if not entity_ID in definitions:
                definitions[entity_ID] = []
            definitions[entity_ID].append(definition)
    return definitions

def loadDefinitions(definitions, db):
    entity_definitions = []
    for (entity_key, defns) in definitions.items():
        for defn in defns:
            entity_definitions.append(EntityDefinition(
                entity_key=entity_key,
                definition=defn
            ))
    db.insertOrUpdate(entity_definitions)



if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-d', '--definitions', dest='definitionsf',
            help='(required) tab-separated file mapping entity IDs to definitions')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.definitionsf:
            parser.print_help()
            parser.error('Must provide --definitions')
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Definitions file', options.definitionsf),
        ('Configuration file', options.configf),
    ], 'Loading definitions into DB')

    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    config = config['PairedNeighborhoodAnalysis']
    log.writeln('Done.\n')

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(config['DatabaseFile'])
    log.writeln('Database ready.\n')

    log.writeln('Loading definitions from %s...' % options.definitionsf)
    definitions = readDefinitions(options.definitionsf)
    log.writeln('Loaded {0:,} definitions for {1:,} entities.'.format(
        sum([len(v) for (k,v) in definitions.items()]),
        len(definitions)
    ))

    log.writeln('Adding to database...')
    loadDefinitions(
        definitions,
        db
    )
    log.writeln('Done.')

    log.stop()
