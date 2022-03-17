from hedgepig_logger import log
from textessence.core.lib import normalization

def preprocessSnapshotCorpus(corpus, normalization_options):
    log.track(message='  >> Processed {0:,} paragraphs', writeInterval=100)
    normalizer = normalization.buildNormalizer(normalization_options)
    with open(corpus.raw_corpus_file, 'r') as in_stream, \
         open(corpus.preprocessed_corpus_file(normalization_options), 'w') as out_stream:
        for line in in_stream:
            for normalized_sentence in normalizer.tokenizeAndNormalize(line):
                out_stream.write('%s\n' % (' '.join(normalized_sentence)))
            log.tick()
    log.flushTracker()
