import configparser
from ...lib import normalization
from ...terminology import TerminologyEnvironment
from ...terminology.terminology_data_models import *
from ...corpus.snapshot_data_models import *

class JETEnvironment(TerminologyEnvironment):
    snapshots_config = None
    snapshots_config_f = None
    snapshots_collection = None

    snapshot_config = None
    snapshot_corpus = None

    normalization_options = None
    JET_path = None

def initializeJETEnvironment(
    config_f,
    terminology,
    snapshot=None
):
    env = JETEnvironment()

    env.base_config = configparser.ConfigParser()
    env.base_config_f = config_f
    env.base_config.read(env.base_config_f)

    env.term_config = configparser.ConfigParser()
    env.term_config_f = env.base_config['General']['TerminologyConfig']
    env.term_config.read(env.term_config_f)

    root_dir = env.term_config['Default']['RootDirectory']
    env.terminology_collection = TerminologyCollection(root_dir)
    env.terminology = env.terminology_collection.get(terminology, None)

    env.snapshots_config = configparser.ConfigParser()
    env.snapshots_config.read(env.base_config['General']['SnapshotConfig'])

    if snapshot:
        snapshots_root_dir = env.snapshots_config['Default']['SnapshotsRootDirectory']
        env.snapshot_config = env.snapshots_config[snapshot]
        env.snapshot_root_dir = env.snapshot_config['RootDirectory']

        env.snapshots_collection = LiteratureSnapshotCollection(
            snapshots_root_dir
        )
        env.snapshot_corpus = LiteratureSnapshotCorpus(
            snapshot,
            env.snapshot_config['RootDirectory']
        )

    env.normalization_options = normalization.loadConfiguration(env.base_config['Normalization'])
    env.JET_path = env.base_config['General']['JETInstallation']

    return env
