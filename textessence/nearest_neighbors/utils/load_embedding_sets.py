import csv
import configparser
from hedgepig_logger import log
from ..data_models import *
from ..database import EmbeddingNeighborhoodDatabase

def readEmbeddingSets(f, delimiter=','):
    groups_by_short_name, sets = {}, []
    with open(f, 'r') as stream:
        reader = csv.DictReader(stream, delimiter=delimiter)
        for record in reader:
            group = record['Group']
            if not group in groups_by_short_name:
                groups_by_short_name[group] = EmbeddingSetGroup(
                    short_name=group,
                    display_title=group
                )
            group = groups_by_short_name[group]

            sets.append(EmbeddingSet(
                group=group,
                name=record['Name'],
                ordering=int(record['Ordering'])
            ))
    return sets

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
        parser.add_option('-i', '--input', dest='embedding_setsf',
            help='(required) CSV file with header containing embedding sets to load')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.embedding_setsf:
            parser.print_help()
            parser.error('Must provide --input')
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Embedding sets file', options.embedding_setsf),
        ('Configuration file', options.configf),
    ], 'Loading embedding sets into DB')

    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    config = config['PairedNeighborhoodAnalysis']
    log.writeln('Done.\n')

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(config['DatabaseFile'])
    log.writeln('Database ready.\n')

    log.writeln('Loading embedding set specifications from %s...' % options.embedding_setsf)
    sets = readEmbeddingSets(options.embedding_setsf)
    log.writeln('Loaded {0:,} embedding sets for {1:,} groups.'.format(
        len(sets),
        len(set([s.group.short_name for s in sets]))
    ))

    log.writeln('Adding to database...')
    db.insertOrUpdate(sets)
    log.writeln('Done.')

    log.stop()
