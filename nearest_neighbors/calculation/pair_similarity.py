import configparser
import numpy as np
import pyemblib
from hedgepig_logger import log
import threading
from .. import nn_io
from ..data_models import AggregatePairwiseSimilarity
from ..database import EmbeddingNeighborhoodDatabase


def calculateAggregatePairwiseSimilarity(group, replicates, query, target, db):
    cos_sims = []
    for these_embeds in replicates:
        query_vec = these_embeds[query]
        target_vec = these_embeds[target]

        query_vec = query_vec / np.linalg.norm(query_vec)
        target_vec = target_vec / np.linalg.norm(target_vec)

        cos_sim = np.dot(query_vec, target_vec)
        cos_sims.append(cos_sim)

    sim = AggregatePairwiseSimilarity(
        source=replicates.source,
        key=query,
        neighbor_key=target,
        mean_similarity=np.mean(cos_sims),
        std_similarity=np.std(cos_sims)
    )
    db.insertOrUpdate(sim)

    return sim

def _getExistingAggregatePairwiseSimilarity(group, query, target, db, embedding_sets):
    """Fetches any pre-existing aggregate pairwise similarity calculations for
    the given pair of concepts."""
    sims = {}

    # find any similarities which have already been calculated
    rows = list(db.selectFromAggregatePairwiseSimilarity(query, target))
    for sim in rows:
        sims[sim.source.name] = sim
    # check for any which may be reversed (as similarity is symmetric)
    if len(sims) < len(embedding_sets):
        rows = list(db.selectFromAggregatePairwiseSimilarity(target, query))
        for sim in rows:
            sims[sim.source.name] = sim
            
    return sims

def calculateAllAggregatePairwiseSimilaritiesBackground(group, query, target, config, db):
    """
    Manages a background process to compute aggregate pairwise similarities.
    Returns a dictionary containing a "result" key (which will be None if the
    result isn't available yet) and, if "result" is None, a "progress" key
    containing a PairwiseSimilarityProgress object.
    """
    # Check if there's an ongoing job
    progress = db.getPairwiseSimilarityProgress(group, query, target)
    if progress and progress.running:
        return {"result": None, "progress": progress}
    
    embedding_sets = list(db.selectFromEmbeddingSets(group_ID=group.ID))

    sims = _getExistingAggregatePairwiseSimilarity(group, query, target, db, embedding_sets)
    missing = False
    ordered_sims = []
    for emb_set in sorted(embedding_sets, key=lambda es: es.ordering):
        if emb_set.name in sims:
            ordered_sims.append(sims[emb_set.name])
        else:
            # Values missing - start a job
            missing = True
            break

    if missing:
        progress = db.updatePairwiseSimilarityProgress(group, query, target, True, 0.0, "Computing similarity...")
        thread = threading.Thread(target=_pairwiseSimilaritiesWorker,
                                args=(group, query, target, config))
        thread.start()
        return {"result": None, "progress": progress}

    return {"result": ordered_sims}
    
def _pairwiseSimilaritiesWorker(group, query, target, config):
    """Runs in a background thread"""
    db = EmbeddingNeighborhoodDatabase(config['PairedNeighborhoodAnalysis']['DatabaseFile'], build=False)
    calculateAllAggregatePairwiseSimilarities(group, query, target, config, db)
    db.close()
    
def calculateAllAggregatePairwiseSimilarities(group, query, target, config, db):
    embedding_sets = list(db.selectFromEmbeddingSets(group_ID=group.ID))
    sims = _getExistingAggregatePairwiseSimilarity(group, query, target, db, embedding_sets)

    # go through and calculate any that are still missing
    for emb_set in embedding_sets:
        if not emb_set.name in sims:
            emb_set_config = config[emb_set.name]
            log.writeln('Loading embedding replicates for %s...' % emb_set.name)
            db.updatePairwiseSimilarityProgress(group, 
                                                query, 
                                                target, 
                                                True,
                                                len(sims) / len(embedding_sets), 
                                                "Corpus {} of {}...".format(len(sims) + 1, len(embedding_sets)))
            replicates = nn_io.EmbeddingReplicates(
                emb_set,
                emb_set_config['ReplicateTemplate'].format(REPL='*'),
                emb_set_config['EmbeddingFormat']
            )
            log.writeln('Found {0:,} replicates.\n'.format(len(replicates)))

            if not replicates.hasKey(query) or not replicates.hasKey(target):
                log.writeln('{0} replicates missing one of "{1}"/"{2}", skipping'.format(emb_set.name, query, target))
                sim = AggregatePairwiseSimilarity(
                    source=replicates.source,
                    key=query,
                    neighbor_key=target,
                    mean_similarity=None,
                    std_similarity=None
                )
                db.insertOrUpdate(sim)
                sims[emb_set.name] = sim
            else:
                t = log.startTimer('Calculating aggregate pairwise similarity...')
                sim = calculateAggregatePairwiseSimilarity(
                    group,
                    replicates,
                    query,
                    target,
                    db
                )
                log.stopTimer(t, 'Done in {0:.2f}s.')
                sims[emb_set.name] = sim

    db.updatePairwiseSimilarityProgress(group, 
                                        query, 
                                        target, 
                                        False,
                                        1.0, 
                                        "Complete")
    log.writeln('Done calculating pairwise similarities.')
    
    ordered_sims = []
    for emb_set in sorted(embedding_sets, key=lambda es: es.ordering):
        if emb_set.name in sims:
            ordered_sims.append(sims[emb_set.name])

    return ordered_sims


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

    sims = calculateAllAggregatePairwiseSimilarities(
        group,
        options.query_key,
        options.target_key,
        config,
        db
    )

    for sim in sims:
        log.writeln('Source: {0}'.format(sim.source.name))
        log.writeln('  Mean similarity: {0:.4f}'.format(sim.mean_similarity))
        log.writeln('  Similarity std dev: {0:.4f}\n'.format(sim.std_similarity))

    log.stop()
