import configparser
import numpy as np
from hedgepig_logger import log
from .. import nn_io
from ..data_models import *
from ..database import EmbeddingNeighborhoodDatabase
from .paired_neighborhood_overlap import pairedOverlapDistributions

def analyzeInternalConfidence(src, config, db, k=5):
    src_neighbor_sets = []

    log.track('  >> [1/3] Loaded {0:,}/10 neighbor sets')
    for i in range(1,11):
        src_neighbor_sets.append(nn_io.loadPairedNeighbors(
            src,
            i,
            src,
            config,
            k=k
        ))
        log.tick()
    log.flushTracker()
    
    log.writeln('  >> [2/3] Calculating source self overlaps...')
    src_self_distribs = pairedOverlapDistributions(
        src_neighbor_sets,
        src_neighbor_sets,
        self_paired=True,
        loglbl='SRC SELF '
    )

    log.writeln('  >> [3/3] Adding overlap analyses to database...')
    confidences = []
    for key in src_self_distribs:
        confidences.append(InternalConfidence(
            source=src,
            at_k=k,
            key=key,
            confidence=src_self_distribs[key]
        ))
    db.insertOrUpdate(confidences)


if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-s', '--src', dest='src',
            help='(required) source specifier')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-k', '--nearest-neighbors', dest='k',
            help='number of nearest neighbors to use in statistics (default: %default)',
            type='int', default=5)
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.src:
            parser.print_help()
            parser.error('Must provide --src')
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Source specifier', options.src),
        ('Configuration file', options.configf),
        ('Number of nearest neighbors to analyze', options.k),
    ], 'Paired neighborhood analysis')

    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    config = config['PairedNeighborhoodAnalysis']
    log.writeln('Done.\n')

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(config['DatabaseFile'])
    log.writeln('Database ready.\n')

    log.writeln('Analyzing {0} internal confidence...'.format(options.src))
    analyzeInternalConfidence(
        options.src,
        config,
        db,
        k=options.k
    )
    log.writeln('Extracted statistics.\n')

    log.stop()
