import configparser
import numpy as np
from hedgepig_logger import log
from .. import nn_io
from ..data_models import *
from ..database import EmbeddingNeighborhoodDatabase

def getNeighborhoodOverlap(neighbors_1, neighbors_2, loglbl=''):
    overlap_percentages = {}
    ## TODO: under current workflow, union vs intersection should be identical here.
    ## However, should really check that.
    total_keys = set(neighbors_1.keys()).union(set(neighbors_2.keys()))
    #log.writeln('[INFO] Neighbors 1 -- %d keys' % len(neighbors_1))
    #log.writeln('[INFO] Neighbors 2 -- %d keys' % len(neighbors_2))
    #log.writeln('[INFO] Union -- %d keys' % len(total_keys))

    for key in total_keys:
        nbr_info_1 = neighbors_1.get(key, [])
        nbr_info_2 = neighbors_2.get(key, [])
        nbrs_1 = set([nbr_ID for (nbr_ID, dist) in nbr_info_1])
        nbrs_2 = set([nbr_ID for (nbr_ID, dist) in nbr_info_2])
        overlap_count = len(nbrs_1.intersection(nbrs_2))
        overlap = overlap_count / (max(len(nbrs_1), len(nbrs_2)))
        #log.writeln('[DEBUG] %sKey "%s"  Neighbors 1: [%s]  Neighbors 2: [%s]  Overlap: %.1f%%' % (
        #    loglbl, key, ','.join(nbrs_1), ','.join(nbrs_2), 100*overlap
        #))
        overlap_percentages[key] = overlap

    return overlap_percentages

def pairedOverlapDistributions(neighbor_sets_1, neighbor_sets_2, self_paired=False, loglbl=''):
    # take all pairs of neighbor sets to compare
    #  if self_paired (i.e., comparing neighbors pulled from runs for the same
    #    subset), only do the unique pairs
    #  otherwise, take full cross-product
    overlap_percentage_samples = {}
    for i in range(len(neighbor_sets_1)):
        inner_loop_start = (i+1) if self_paired else 0
        for j in range(inner_loop_start, len(neighbor_sets_2)):
            overlap_percentages = getNeighborhoodOverlap(
                neighbor_sets_1[i],
                neighbor_sets_2[j],
                loglbl=loglbl
            )

            for (key, perc) in overlap_percentages.items():
                if not key in overlap_percentage_samples:
                    overlap_percentage_samples[key] = []
                overlap_percentage_samples[key].append(perc)

    # squish overlap distributions for each individual key down to its mean
    overlap_percentage_means = {
        key: np.mean(samples)
            for (key, samples) in overlap_percentage_samples.items()
    }

    return overlap_percentage_means

def rankKeysByMeanDeltaFromBaseline(control_overlap_percentage_means,
        experimental_overlap_percentage_means):
    keys = set(control_overlap_percentage_means.keys()) \
        .intersection(set(experimental_overlap_percentage_means.keys()))

    deltas = {
        key: (
            control_overlap_percentage_means[key]
            - experimental_overlap_percentage_means[key]
        )
            for key in keys
    }

    sorted_deltas = sorted(
        deltas.items(),
        key=lambda k:k[1],
        reverse=True
    )

    return sorted_deltas

def loadInNeighbors(src, i, trg, config, k):
    neighbor_file = config['NeighborFilePattern'].format(
        SRC=src, SRC_RUN=i, TRG=trg
    )
    neighbor_vocab = config['NeighborVocabFilePattern'].format(
        SRC=src, SRC_RUN=i, TRG=trg
    )

    #log.writeln('\nREADING NODE MAP FROM %s' % neighbor_vocab)
    node_map = nn_io.readNodeMap(neighbor_vocab)

    #log.writeln('READING NODES FROM %s' % neighbor_file)
    neighbors = nn_io.readNeighborFile(
        neighbor_file,
        k=k,
        node_map=node_map,
        with_distances=True
    )

    return neighbors


def analyzeOverlap(src, trg, config, db, k=5,
        confidence_threshold=0.5):
    src_neighbor_sets = []
    trg_neighbor_sets = []

    log.track('  >> [1/7] Loaded {0:,}/10 neighbor sets')
    for i in range(1,11):
        src_neighbor_sets.append(loadInNeighbors(
            src,
            i,
            trg,
            config,
            k=k
        ))
        trg_neighbor_sets.append(loadInNeighbors(
            trg,
            i,
            src,
            config,
            k=k
        ))
        log.tick()
    log.flushTracker()
    
    log.writeln('  >> [2/7] Calculating source self overlaps...')
    src_self_distribs = pairedOverlapDistributions(
        src_neighbor_sets,
        src_neighbor_sets,
        self_paired=True,
        loglbl='SRC SELF '
    )
    log.writeln('  >> [3/7] Calculating target self overlaps...')
    trg_self_distribs = pairedOverlapDistributions(
        trg_neighbor_sets,
        trg_neighbor_sets,
        self_paired=True,
        loglbl='TRG SELF '
    )
    log.writeln('  >> [4/7] Calculating cross overlaps...')
    cross_distribs = pairedOverlapDistributions(
        src_neighbor_sets,
        trg_neighbor_sets,
        self_paired=False,
        loglbl='CROSS '
    )

    log.writeln('  >> [5/7] Adding overlap analyses to database...')
    all_keys = set(src_self_distribs.keys())\
        .union(set(trg_self_distribs.keys()))\
        .union(set(cross_distribs.keys()))

    overlaps = []
    for key in all_keys:
        overlaps.append(EntityOverlapAnalysis(
            source=src,
            target=trg,
            at_k=k,
            key=key,
            source_confidence=src_self_distribs[key],
            target_confidence=trg_self_distribs[key],
            EN_similarity=cross_distribs[key]
        ))
    db.insertOrUpdate(overlaps)


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
        parser.add_option('-m', '--string-map', dest='string_mapf',
            help='file mapping embedding keys to strings')
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
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Source specifier', options.src),
        ('Target specifier', options.trg),
        ('Configuration file', options.configf),
        ('Number of nearest neighbors to analyze', options.k),
        ('String map file', options.string_mapf)
    ], 'Paired neighborhood analysis')

    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    config = config['PairedNeighborhoodAnalysis']
    log.writeln('Done.\n')

    if options.string_mapf:
        log.writeln('Reading string map from %s...' % options.string_mapf)
        string_map = nn_io.readStringMap(options.string_mapf, lower_keys=True)
        log.writeln('Mapped strings for {0:,} keys.\n'.format(len(string_map)))
    else:
        string_map = None

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(config['DatabaseFile'])
    log.writeln('Database ready.\n')

    log.writeln('Analyzing {0}/{1} neighbors...'.format(options.src, options.trg))
    analyzeOverlap(
        options.src,
        options.trg,
        config,
        db,
        k=options.k
    )
    log.writeln('Extracted statistics.\n')

    log.stop()
