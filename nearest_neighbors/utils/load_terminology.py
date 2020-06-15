import configparser
from hedgepig_logger import log
from ..data_models import *
from ..database import EmbeddingNeighborhoodDatabase

def readTerminology(f):
    terminology = {}
    with open(f, 'r') as stream:
        for line in stream:
            (entity_ID, term) = [s.strip() for s in line.split('\t', 1)]
            if not entity_ID in terminology:
                terminology[entity_ID] = []
            terminology[entity_ID].append(term)
    return terminology

def loadTerminology(terminology, db):
    entity_terms = []
    for (entity_key, terms) in terminology.items():
        # default first term to be preferred
        terms.reverse()
        for i in range(len(terms)):
            preferred = 1 if (i==len(terms)-1) else 0
            entity_terms.append(EntityTerm(
                entity_key=entity_key,
                term=terms[i],
                preferred=preferred
            ))
    db.insertOrUpdate(entity_terms)



if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-t', '--terminology', dest='terminologyf',
            help='(required) tab-separated terminology file mapping entity IDs to terms')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.terminologyf:
            parser.print_help()
            parser.error('Must provide --terminology')
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Terminology file', options.terminologyf),
        ('Configuration file', options.configf),
    ], 'Loading terminology into DB')

    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    config = config['PairedNeighborhoodAnalysis']
    log.writeln('Done.\n')

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(config['DatabaseFile'])
    log.writeln('Database ready.\n')

    log.writeln('Loading terminology from %s...' % options.terminologyf)
    terminology = readTerminology(options.terminologyf)
    log.writeln('Loaded {0:,} terms for {1:,} entities.'.format(
        sum([len(v) for (k,v) in terminology.items()]),
        len(terminology)
    ))

    log.writeln('Adding to database...')
    loadTerminology(
        terminology,
        db
    )
    log.writeln('Done.')

    log.stop()
