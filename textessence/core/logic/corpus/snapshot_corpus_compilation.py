import os
import configparser
from hedgepig_logger import log
from textessence.core.models.snapshot_data_models import *

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

        self._has_fresh_metadata = False

    @property
    def previous_version_exists(self):
        return len(self.reference_corpus.document_data) > 0

    @property
    def changed_since_last_compile(self):
        if not self._has_fresh_metadata:
            self.compileFreshMetadata()
        return areCorporaDifferent(
            self.corpus.document_data,
            self.reference_corpus.document_data
        )

    def compileFreshMetadata(self):
        for snapshot_lbl in self.snapshots:
            snapshot = self.collection[snapshot_lbl]
            log.writeln('>>> Processing snapshot {0} ({1:,} documents) <<<'.format(
                snapshot_lbl, len(snapshot)
            ))
            log.track('  >> Pulled data for {0:,} documents', writeInterval=100)
            for doc in snapshot:
                included_abstract, included_full_text = False, False
                if doc.has_abstract:
                    included_abstract = True
                if (not self.abstracts_only) and doc.has_full_text:
                    included_full_text = True
                if included_abstract or included_full_text:
                    self.corpus.addDocument(
                        doc,
                        included_abstract,
                        included_full_text,
                        flush=False
                    )
                    log.tick()
            log.flushTracker()
        self._has_fresh_metadata = True

def runSnapshotCorpusCompilation(compilation_data):
    log.writeln('== [1] Compiling current metadata for snapshot corpus ==')
    compilation_data.compileFreshMetadata()

    if compilation_data.previous_version_exists:
        log.writeln('== [2] Previous version of corpus found, checking for changes ==')
        if compilation_data.changed_since_last_compile:
            log.writeln('Difference found! Proceeding with re-compilation.')
        else:
            log.writeln('No change found. Exiting.')
            return
    else:
        log.writeln('== [2] No previous version of corpus found, continuing with compilation ==')
    log.writeln()

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
    with open(compilation_data.corpus.raw_corpus_file, 'w') as stream:
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
