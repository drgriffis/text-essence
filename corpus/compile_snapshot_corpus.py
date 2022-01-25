import os
import configparser
from hedgepig_logger import log
from .snapshot_data_models import *

class CompilationData:
    def __init__(self, collection, snapshot_label, snapshot_config):
        self.collection = collection

        # this object will be used for compiling the current state
        # of the corpus
        self.corpus = LiteratureSnapshotCorpus(
            snapshot_label,
            snapshot_config['RootDirectory']
        )
        # this object is used for pulling the existing state of
        # the corpus (if it already exists)
        self.reference_corpus = LiteratureSnapshotCorpus(
            snapshot_label,
            snapshot_config['RootDirectory']
        )

        self.snapshots = [s.strip() for s in snapshot_config['Snapshots'].split(',')]
        self.abstracts_only = snapshot_config['AbstractsOnly'].lower().strip() == 'true'

def runSnapshotCorpusCompilation(compilation_data):
    log.writeln('== [1] Compiling current metadata for snapshot corpus ==')
    compileSnapshotCorpusMetadata(
        compilation_data
    )

    if len(compilation_data.reference_corpus.document_data) > 0:
        log.writeln('== [2] Previous version of corpus found, checking for changes ==')
        if areCorporaDifferent(
            compilation_data.corpus.document_data,
            compilation_data.reference_corpus.document_data
        ):
            log.writeln('Difference found! Proceeding with re-compilation.')
        else:
            log.writeln('No change found. Exiting.')
            return
    log.writeln()
        
    else:
        log.writeln('== [2] No previous version of corpus found, continuing with compilation ==')

    log.writeln('== [3] Compiling corpus text ==')
    compileSnapshotCorpus(
        compilation_data
    )

def areCorporaDifferent(doc_data_1, doc_data_2):
    ids_1 = set(doc_data_1.keys())
    ids_2 = set(doc_data_2.keys())
    ids_shared = ids_1.intersection(ids_2)

    if (
        (len(ids_shared) < len(ids_1))
        or (len(ids_shared) < len(ids_2))
    ):
        return True

    for doc_id in ids_shared:
        doc_1 = doc_data_1[doc_id]
        doc_2 = doc_data_2[doc_id]
        if doc_1 != doc_2:
            return True

    return False

def compileSnapshotCorpus(compilation_data):
    with open(compilation_data.corpus.corpus_file, 'w') as stream:
        log.track('  >> Wrote data from {0:,}/%s documents' % (
            '{0:,}'.format(len(compilation_data.corpus.document_data))
        ), writeInterval=10)
        for doc in compilation_data.corpus.document_data.values():
            # handle abstract
            if doc.included_abstract:
                with open(doc.doc_obj.abstract_file, 'r') as in_stream:
                    for line in in_stream:
                        stream.write(line)
            # handle full text
            if doc.included_full_text:
                with open(doc.doc_obj.full_text_file, 'r') as in_stream:
                    for line in in_stream:
                        stream.write(line)
            log.tick()
        log.flushTracker()
    compilation_data.corpus.writeMetadata()

def compileSnapshotCorpusMetadata(compilation_data):
    for snapshot_lbl in compilation_data.snapshots:
        snapshot = compilation_data.collection[snapshot_lbl]
        log.writeln('>>> Processing snapshot {0} ({1:,} documents) <<<'.format(
            snapshot_lbl, len(snapshot)
        ))
        log.track('  >> Pulled data for {0:,} documents', writeInterval=100)
        for doc in snapshot:
            included_abstract, included_full_text = False, False
            if doc.has_abstract:
                included_abstract = True
            if (not compilation_data.abstracts_only) and doc.has_full_text:
                included_full_text = True
            if included_abstract or included_full_text:
                compilation_data.corpus.addDocument(
                    doc,
                    included_abstract,
                    included_full_text,
                    flush=False
                )
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

    config = configparser.ConfigParser()
    config.read(options.config_f)

    snapshots_root_dir = config['TemporalCorpora']['RootDirectory']
    snapshot_config = config['Snapshot {0}'.format(options.snapshot)]
    snapshot_root_dir = snapshot_config['RootDirectory']

    if not os.path.exists(snapshot_root_dir):
        print('Snapshot root directory "{0}" does not exist.'.format(snapshot_root_dir))
        if options.create_if_not_exists:
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
        '{0}.compile_snapshot_corpus.log'.format(options.snapshot)
    )

    log.start(logfile)
    log.writeConfig([
        ('Snapshot collection root directory', snapshots_root_dir),
        ('Snapshot to compile', options.snapshot),
        ('Snapshot configuration', sorted(snapshot_config.items()))
    ], 'Compiling snapshot corpus file')

    collection = LiteratureSnapshotCollection(
        snapshots_root_dir
    )

    compilation_data = CompilationData(
        collection,
        options.snapshot,
        snapshot_config
    )

    runSnapshotCorpusCompilation(
        compilation_data
    )

    log.stop()
