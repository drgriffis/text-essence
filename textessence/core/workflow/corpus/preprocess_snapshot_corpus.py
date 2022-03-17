import os
import configparser
from hedgepig_logger import log
from textessence.lib import normalization
from textessence.core.models.snapshot_data_models import *
from textessence.core.logic.corpus import snapshot_corpus_preprocessing

def preprocessSnapshotCorpus(base_config_filepath, snapshot_name):
    base_config = configparser.ConfigParser()
    base_config.read(base_config_filepath)

    normalization_options = normalization.loadConfiguration(base_config['Normalization'])

    snapshots_config = configparser.ConfigParser()
    snapshots_config_filepath = base_config['General']['SnapshotConfig']
    snapshots_config.read(snapshots_config_filepath)

    snapshots_root_dir = snapshots_config['Default']['SnapshotsRootDirectory']
    snapshot_config = snapshots_config[snapshot_name]
    snapshot_root_dir = snapshot_config['RootDirectory']

    logfile = os.path.join(
        snapshot_root_dir,
        '{0}.preprocess_corpus.log'.format(snapshot_name)
    )

    log.start(logfile)
    log.writeConfig([
        ('Base configuration file', base_config_filepath),
        ('Snapshots configuration file', snapshots_config_filepath),
        ('Snapshot collection root directory', snapshots_root_dir),
        ('Snapshot to compile', snapshot_name),
        ('Snapshot configuration', sorted(snapshot_config.items())),
        ('Normalization options', normalization_options.asLabeledList())
    ], 'CORD-19 corpus preprocessing')

    collection = LiteratureSnapshotCollection(
        snapshots_root_dir
    )
    snapshot_corpus = LiteratureSnapshotCorpus(
        snapshot_name,
        snapshot_config['RootDirectory']
    )

    log.writeln('Preprocessing input corpus %s' % snapshot_corpus.raw_corpus_file)
    snapshot_corpus_preprocessing.preprocessSnapshotCorpus(
        snapshot_corpus,
        normalization_options
    )
    log.writeln('Preprocessed corpus written to %s' % snapshot_corpus.preprocessed_corpus_file(normalization_options))

    log.stop()

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

    preprocessSnapshotCorpus(
        options.config_f,
        options.snapshot
    )
