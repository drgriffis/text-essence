import scispacy
import spacy
from hedgepig_logger import log
from textessence.lib import normalization
from .snapshot_data_models import *

def preprocess(corpus, normalization_options):
    log.track(message='  >> Processed {0:,} paragraphs', writeInterval=100)
    normalizer = normalization.Normalizer(normalization_options)
    with open(corpus.raw_corpus_file, 'r') as in_stream, \
         open(corpus.preprocessed_corpus_file(normalization_options), 'w') as out_stream:
        nlp = spacy.load('en_core_sci_lg')
        for line in in_stream:
            para = nlp(line)
            for sent in para.sents:
                tokens = normalizer.normalize(sent)
                out_stream.write('%s\n' % (' '.join(tokens)))
            log.tick()
    log.flushTracker()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-c', '--config', dest='config_f',
            help='(required) configuration file')
        parser.add_option('-s', '--snapshot', dest='snapshot',
            help='(required) label of snapshot corpus to compile')
        (options, args) = parser.parse_args()
        if not options.config_f:
            parser.print_help()
            parser.error('Must provide --config')
        if not options.snapshot:
            parser.print_help()
            parser.error('Must provide --snapshot')
        return options
    options = _cli()

    base_config = configparser.ConfigParser()
    base_config.read(options.config_f)

    normalization_options = normalization.loadConfiguration(base_config['Normalization'])

    snapshots_config = configparser.ConfigParser()
    snapshots_config.read(base_config['General']['SnapshotConfig']

    snapshots_root_dir = snapshots_config['Default']['SnapshotsRootDirectory']
    snapshot_config = snapshots_config[options.snapshot]
    snapshot_root_dir = snapshot_config['RootDirectory']

    logfile = os.path.join(
        snapshot_root_dir,
        '{0}.preprocess_corpus.log'.format(options.snapshot)
    )

    log.start(logfile)
    log.writeConfig([
        ('Snapshot collection root directory', snapshots_root_dir),
        ('Snapshot to compile', options.snapshot),
        ('Snapshot configuration', sorted(snapshot_config.items())),
        ('Normalization options', normalization_options.asLabeledList())
    ], 'CORD-19 corpus preprocessing')

    collection = LiteratureSnapshotCollection(
        snapshots_root_dir
    )
    snapshot_corpus = LiteratureSnapshotCorpus(
        options.snapshot,
        snapshot_config['RootDirectory']
    )

    log.writeln('Preprocessing input corpus %s' % snapshot_corpus.raw_corpus_file)
    preprocess(
        snapshot_corpus,
        normalization_options
    )
    log.writeln('Preprocessed corpus written to %s' % snapshot_corpus.preprocessed_corpus_file(normalization_options))

    log.stop()
