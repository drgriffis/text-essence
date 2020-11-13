import configparser
from hedgepig_logger import log
from .. import nn_io
from ..data_models import *
from ..database import *

def loadAggregateNeighbors(src, trg, config, db, k=10, neighbor_type=None,
        spec='', filter_spec=''):
    log.writeln('  >> Loading pre-calculated aggregate nearest neighbors')
    aggregate_neighbors = nn_io.loadPairedNeighbors(
        src, None, trg, config, k, aggregate=True, with_distances=True,
        different_types=(not neighbor_type is None), spec=spec,
        filter_spec=filter_spec
    )

    if neighbor_type is None: neighbor_type = EmbeddingType.ENTITY

    log.writeln('  >> Adding to database')
    nbrs = []
    for (key, nbr_list) in aggregate_neighbors.items():
        for (nbr_key, dist) in nbr_list:
            nbrs.append(AggregateNearestNeighbor(
                source=src,
                target=trg,
                filter_set=filter_spec,
                key=key,
                neighbor_key=nbr_key,
                mean_distance=dist
            ))
    db.insertOrUpdate(nbrs, neighbor_type=neighbor_type)


if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-s', '--src', dest='src',
            help='(required) source specifier')
        parser.add_option('-t', '--trg', dest='trg',
            help='(required) target specifier')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-k', '--nearest-neighbors', dest='k',
            help='number of nearest neighbors to use in statistics (default: %default)',
            type='int', default=5)
        parser.add_option('--neighbor-type', dest='neighbor_type',
            help='type of nearest neighbors (if not same as queries)',
            default=None)
        parser.add_option('--neighbor-spec', dest='neighbor_spec',
            default='', help='specifier to help locate neighbor file')
        parser.add_option('--filter-spec', dest='filter_spec',
            default='', help='specifier of key filter set used to generate neighbor file')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.src:
            parser.print_help()
            parser.error('Must provide --src')
        if not options.trg:
            parser.print_help()
            parser.error('Must provide --trg')
        if options.neighbor_type:
            # error check
            _ = EmbeddingType.parse(options.neighbor_type)
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Source specifier', options.src),
        ('Target specifier', options.trg),
        ('Filter specifier', options.filter_spec),
        ('Configuration file', options.configf),
        ('Number of nearest neighbors to add to DB', options.k),
        ('Nearest neighbor type', ('N/A' if options.neighbor_type is None else options.neighbor_type)),
        ('Nearest neighbor file specifier', options.neighbor_spec),
        ('Key filter specifier', options.filter_spec),
    ], 'Loading aggregate neighbors into DB')

    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    config = config['PairedNeighborhoodAnalysis']
    log.writeln('Done.\n')

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(config['DatabaseFile'])
    log.writeln('Database ready.\n')

    neighbor_type = None
    if options.neighbor_type:
        neighbor_type = EmbeddingType.parse(options.neighbor_type)

    log.writeln('Loading aggregate {0}/{1} neighbors into database...'.format(options.src, options.trg))
    loadAggregateNeighbors(
        options.src,
        options.trg,
        config,
        db,
        k=options.k,
        neighbor_type=neighbor_type,
        spec=options.neighbor_spec,
        filter_spec=options.filter_spec
    )
    log.writeln('Done.')

    log.stop()
