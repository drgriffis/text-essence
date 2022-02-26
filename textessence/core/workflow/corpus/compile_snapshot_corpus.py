import os
import configparser
from hedgepig_logger import log
from textessence.core.models.snapshot_data_models import LiteratureSnapshotCollection
from textessence.core.logic.corpus import snapshot_corpus_compilation

def compileSnapshotCorpus(base_config_filepath, snapshot_name, create_if_not_exists=False):
    base_config = configparser.ConfigParser()
    base_config.read(base_config_filepath)

    snapshots_config = configparser.ConfigParser()
    snapshots_config.read(base_config['General']['SnapshotConfig'])

    snapshots_root_dir = snapshots_config['Default']['SnapshotsRootDirectory']
    snapshot_config = snapshots_config[snapshot_name]
    snapshot_root_dir = snapshot_config['RootDirectory']

    if not os.path.exists(snapshot_root_dir):
        print('Snapshot root directory "{0}" does not exist.'.format(snapshot_root_dir))
        if create_if_not_exists:
            print('Running in non-interactive mode, creating directory')
            inpval = 'y'
        else:
            inpval = ''
            while not inpval.strip().lower() in ['y', 'n']:
                inpval = input('Attempt to create it? [y/n] ')
        if inpval.strip().lower() == 'n':
            exit()
        elif inpval.strip().lower() == 'y':
            os.mkdir(snapshot_root_dir)

    logfile = os.path.join(
        snapshot_root_dir,
        '{0}.compile_snapshot_corpus.log'.format(snapshot_name)
    )

    log.start(logfile)
    log.writeConfig([
        ('Snapshot collection root directory', snapshots_root_dir),
        ('Snapshot to compile', snapshot_name),
        ('Snapshot configuration', sorted(snapshot_config.items()))
    ], 'Compiling snapshot corpus file')

    collection = LiteratureSnapshotCollection(
        snapshots_root_dir
    )

    compilation_data = snapshot_corpus_compilation.CompilationData(
        collection,
        snapshot_name,
        snapshot_config
    )

    snapshot_corpus_compilation.runSnapshotCorpusCompilation(
        compilation_data
    )

    log.stop()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-c', '--config', dest='config_f',
            help='(required) configuration file')
        parser.add_option('-s', '--snapshot', dest='snapshot',
            help='(required) label of snapshot corpus to compile')
        parser.add_option('-y', dest='create_if_not_exists',
            action='store_true', default=False,
            help='flag to create extraction directory if it does not exist'
                 ' (otherwise will present interactive prompt)')
        (options, args) = parser.parse_args()
        if not options.config_f:
            parser.print_help()
            parser.error('Must provide --config')
        if not options.snapshot:
            parser.print_help()
            parser.error('Must provide --snapshot')
        return options
    options = _cli()

    compileSnapshotCorpus(
        options.config_f,
        options.snapshot,
        create_if_not_exists=options.create_if_not_exists
    )
