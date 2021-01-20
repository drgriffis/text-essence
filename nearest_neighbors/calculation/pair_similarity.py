import configparser
import numpy as np
import pyemblib
from hedgepig_logger import log
from .. import nn_io
from ..data_models import AggregatePairwiseSimilarity
from ..database import EmbeddingNeighborhoodDatabase


def calculateAggregatePairwiseSimilarity(group, replicates, query, target, db):
    cos_sims = []
    for these_embeds in replicates:
        #t_sub = log.startTimer('Calculating similarity in embedding set %d (%s)...' % (i, embedfs[i]))
        #these_embeds = pyemblib.read(embedfs[i], mode=options.embedding_mode, errors='replace')

        query_vec = these_embeds[query]
        target_vec = these_embeds[target]

        query_vec = query_vec / np.linalg.norm(query_vec)
        target_vec = target_vec / np.linalg.norm(target_vec)

        cos_sim = np.dot(query_vec, target_vec)
        cos_sims.append(cos_sim)

    source_set = db.getOrCreateEmbeddingSet(name=replicates.ID, group_name=group)

    sim = AggregatePairwiseSimilarity(
        source=source_set,
        key=query,
        neighbor_key=target,
        mean_similarity=np.mean(cos_sims),
        std_similarity=np.std(cos_sims)
    )
    db.insertOrUpdate(sim)

    return sim


if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-g', '--group', dest='group',
            help='(required) embedding set group specifier')
        parser.add_option('-s', '--src', dest='src',
            help='(required) source specifier (may provide more than one as'
                 ' comma-separated list)')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-q', '--query', dest='query_key',
            help='(required) query key')
        parser.add_option('-t', '--target', dest='target_key',
            help='(required) target key')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()

        if not options.group:
            parser.print_help()
            parser.error('Must provide --group')
        if not options.query_key:
            parser.print_help()
            parser.error('Must provide --query')
        if not options.target_key:
            parser.print_help()
            parser.error('Must provide --target')

        return args, options

    embedfs, options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Group specifier', options.group),
        ('Source specifier (comma-separated)', options.src),
        ('Configuraiton file', options.configf),
        ('Query key', options.query_key),
        ('Target key', options.target_key),
    ], 'Aggregate pairwise similarity calculation')


    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    analysis_config = config['PairedNeighborhoodAnalysis']
    log.writeln('Done.\n')

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(analysis_config['DatabaseFile'])
    log.writeln('Database ready.\n')

    for src in options.src.split(','):
        src_config = config[src]
        log.writeln('Loading embedding replicates...')
        replicates = nn_io.EmbeddingReplicates(
            src,
            src_config['ReplicateTemplate'].format(REPL='*'),
            src_config['EmbeddingFormat'])
        log.writeln('Found {0:,} replicates.\n'.format(len(replicates)))

        t = log.startTimer('Calculating aggregate pairwise similarity...')
        sim = calculateAggregatePairwiseSimilarity(
            group,
            replicates,
            options.query_key,
            options.target_key,
            db
        )
        log.stopTimer(t, 'Done in {0:.2f}s.')
        log.writeln('  Mean similarity: {0:.4f}'.format(sim.mean_similarity))
        log.writeln('  Similarity std dev: {0:.4f}\n'.format(sim.std_similarity))

    log.stop()
