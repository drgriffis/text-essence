import configparser
import numpy as np
import csv
from hedgepig_logger import log
from .. import nn_io
from ..data_models import *
from ..database import EmbeddingNeighborhoodDatabase
from .paired_neighborhood_overlap import pairedOverlapDistributions

def analyzeInternalConfidence(group, src, config, db, k=5, outf=None, filter_spec=''):
    src_neighbor_sets = []

    source_set = db.getOrCreateEmbeddingSet(name=src, group_name=group)

    log.track('  >> [1/3] Loaded {0:,}/10 neighbor sets')
    for i in range(1,11):
        src_neighbor_sets.append(nn_io.loadPairedNeighbors(
            src,
            i,
            src,
            config,
            k=k,
            filter_spec=filter_spec
        ))
        log.tick()
    log.flushTracker()
    
    log.writeln('  >> [2/3] Calculating source self overlaps...')
    src_self_distribs = pairedOverlapDistributions(
        src_neighbor_sets,
        src_neighbor_sets,
        self_paired=True
    )

    log.writeln('  >> [3/3] Adding overlap analyses to database...')
    confidences = []
    for key in src_self_distribs:
        confidences.append(InternalConfidence(
            source=source_set,
            at_k=k,
            key=key,
            confidence=src_self_distribs[key]
        ))
    db.insertOrUpdate(confidences)

    if outf:
        log.writeln('  >> [BONUS] Writing confidence values to %s...' % outf)
        with open(outf, 'w') as stream:
            writer = csv.writer(stream)
            for conf in confidences:
                writer.writerow([conf.key, '{0:.3f}'.format(conf.confidence)])


if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-g', '--group', dest='group',
            help='(required) embedding set group specifier')
        parser.add_option('-s', '--src', dest='src',
            help='(required) source specifier')
        parser.add_option('--filter-spec', dest='filter_spec',
            default='',
            help='(optional) filter specifier')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-k', '--nearest-neighbors', dest='k',
            help='number of nearest neighbors to use in statistics (default: %default)',
            type='int', default=5)
        parser.add_option('--dump', dest='dumpf',
            help='(optional) file to dump confidence values to (in addition to DB export)')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.group:
            parser.print_help()
            parser.error('Must provide --group')
        if not options.src:
            parser.print_help()
            parser.error('Must provide --src')
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Group specifier', options.group),
        ('Source specifier', options.src),
        ('Filter specifier', options.filter_spec),
        ('Configuration file', options.configf),
        ('Number of nearest neighbors to analyze', options.k),
        ('Output dump file', '--unused--' if not options.dumpf else options.dumpf),
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
        options.group,
        options.src,
        config,
        db,
        k=options.k,
        outf=options.dumpf,
        filter_spec=options.filter_spec
    )
    log.writeln('Extracted statistics.\n')

    log.stop()
