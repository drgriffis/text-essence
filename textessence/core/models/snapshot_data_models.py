import os
import csv
from hedgepig_logger import log
from textessence.lib import normalization

class LiteratureSnapshotCollection:
    snapshots = None

    def __init__(self, root_directory):
        self.root_directory = root_directory
        self.snapshots = {}
        self._loadMetadata()

    def __iter__(self):
        return iter(self.snapshots.values())
    def __contains__(self, key):
        return key in self.snapshots
    def __getitem__(self, key):
        return self.snapshots[key]

    def createSnapshot(self, key):
        if key in self.snapshots:
            raise KeyError('Snapshot key "{0}" already exists'.format(key))

        snapshot_root_dir = os.path.join(self.root_directory, key)
        snapshot = LiteratureSnapshot(
            key,
            snapshot_root_dir,
            setup=True
        )

        self.snapshots[key] = snapshot
        self._saveMetadata()

    def listSnapshots(self):
        return list(self.snapshots.values())

    @property
    def _metadata_path(self):
        return os.path.join(self.root_directory, 'collection_metadata.csv')
    def _loadMetadata(self):
        if os.path.exists(self._metadata_path):
            with open(self._metadata_path, 'r') as stream:
                reader = csv.DictReader(stream)
                for record in reader:
                    self.snapshots[record['SnapshotKey']] = LiteratureSnapshot(
                        record['SnapshotKey'],
                        record['RootDirectory']
                    )
    def _saveMetadata(self):
        with open(self._metadata_path, 'w') as stream:
            writer = csv.DictWriter(stream, fieldnames=[
                'SnapshotKey',
                'RootDirectory'
            ])
            writer.writeheader()
            for (snapshot_key, snapshot) in self.snapshots.items():
                writer.writerow({
                    'SnapshotKey': snapshot_key,
                    'RootDirectory': snapshot.root_directory
                })

class LiteratureSnapshot:
    label = None
    root_directory = None

    def __init__(self, label, root_directory, setup=False):
        self.label = label
        self.root_directory = root_directory
        self.documents = {}

        if not os.path.exists(self.root_directory):
            if setup:
                self._setup()
            else:
                log.writeln('[WARNING] Initialized LiteratureSnapshot with non-existent root directory {0}'.format(self.root_directory))

        self.loadMetadata()

    def _setup(self):
        if not os.path.exists(self.root_directory):
            os.mkdir(self.root_directory)
        if not os.path.exists(self.abstract_text_dir):
            os.mkdir(self.abstract_text_dir)
        if not os.path.exists(self.full_text_dir):
            os.mkdir(self.full_text_dir)

    def loadMetadata(self):
        if os.path.isfile(self.metadata_file):
            with open(self.metadata_file, 'r') as stream:
                reader = csv.DictReader(stream)
                for record in reader:
                    self.documents[record['ID']] = LiteratureSnapshotDocument(
                        ID=record['ID'],
                        has_abstract=(record['HasAbstract'].lower() == 'true'),
                        has_full_text=(record['HasFullText'].lower() == 'true'),
                        dump_date=record['DumpDate'],
                        publication_date=record['PublicationDate'],
                        date_resolution_status=record['DateResolutionStatus'],
                        resolved_date=record['ResolvedDate'],
                        parent_snapshot=self
                    )

    def writeMetadata(self):
        with open(self.metadata_file, 'w') as stream:
            writer = csv.DictWriter(stream, fieldnames=[
                'ID',
                'HasAbstract',
                'HasFullText',
                'DumpDate',
                'PublicationDate',
                'DateResolutionStatus',
                'ResolvedDate'
            ])
            writer.writeheader()
            for document in self:
                writer.writerow({
                    'ID': document.ID,
                    'HasAbstract': str(document.has_abstract),
                    'HasFullText': str(document.has_full_text),
                    'DumpDate': document.dump_date,
                    'PublicationDate': document.publication_date,
                    'DateResolutionStatus': document.date_resolution_status,
                    'ResolvedDate': self.label
                })

    def addDocument(self, document, flush=True):
        self.documents[document.ID] = document
        document.parent_snapshot = self
        if flush: self.writeMetadata()

    def __iter__(self):
        return iter(self.documents.values())
    def __getitem__(self, key):
        return self.documents[key]
    def __len__(self):
        return len(self.documents)

    def abstractTextPath(self, document):
        return os.path.join(self.abstract_text_dir, '{0}.txt'.format(document.ID))
    def fullTextPath(self, document):
        return os.path.join(self.full_text_dir, '{0}.txt'.format(document.ID))

    @property
    def metadata_file(self):
        return os.path.join(self.root_directory, 'metadata.csv')
    @property
    def abstract_text_dir(self):
        return os.path.join(self.root_directory, 'abstract_text')
    @property
    def full_text_dir(self):
        return os.path.join(self.root_directory, 'full_text')

class LiteratureSnapshotDocument:
    ID = None
    dump_date = None
    publication_date = None
    date_resolution_status = None
    parent_snapshot = None
    has_abstract = False
    has_full_text = False
    
    def __init__(self, ID, parent_snapshot=None, dump_date=None,
            publication_date=None, date_resolution_status=None,
            resolved_date=None, has_abstract=False,
            has_full_text=False):
        self.ID = ID
        self.dump_date = dump_date
        self.publication_date = publication_date
        self.date_resolution_status = date_resolution_status
        self.parent_snapshot = parent_snapshot
        self.has_abstract = has_abstract
        self.has_full_text = has_full_text

    @property
    def abstract_file(self):
        if self.has_abstract:
            return self.parent_snapshot.abstractTextPath(self)
        else: return None
    @property
    def full_text_file(self):
        if self.has_full_text:
            return self.parent_snapshot.fullTextPath(self)
        else: return None

    @property
    def snapshot_label(self):
        return self.parent_snapshot.label

class LiteratureSnapshotCorpus:
    label = None
    root_directory = None
    document_data = None

    def __init__(self, label, root_directory):
        self.label = label
        self.root_directory = root_directory
        self.document_data = {}

        self.loadMetadata()

    def addDocument(self, document, included_abstract, included_full_text, flush=True):
        self.document_data[document.ID] = LiteratureSnapshotCorpusDocument(
            ID=document.ID,
            source=document.snapshot_label,
            included_abstract=included_abstract,
            included_full_text=included_full_text,
            doc_obj=document
        )
        if flush:
            self.writeMetadata()

    def writeMetadata(self):
        with open(self.metadata_file, 'w') as stream:
            metadata_writer = csv.DictWriter(stream, fieldnames=[
                'ID',
                'Source',
                'IncludedAbstract',
                'IncludedFullText'
            ])
            metadata_writer.writeheader()
            for document in self.document_data.values():
                metadata_writer.writerow({
                    'ID': document.ID,
                    'Source': document.source,
                    'IncludedAbstract': document.included_abstract,
                    'IncludedFullText': document.included_full_text
                })

    def loadMetadata(self):
        if os.path.isfile(self.metadata_file):
            with open(self.metadata_file, 'r') as stream:
                reader = csv.DictReader(stream)
                for record in reader:
                    self.document_data[record['ID']] = LiteratureSnapshotCorpusDocument(
                        ID=record['ID'],
                        source=record['Source'],
                        included_abstract=(record['IncludedAbstract'].lower() == 'true'),
                        included_full_text=(record['IncludedFullText'].lower() == 'true')
                    )

    @property
    def raw_corpus_file(self):
        return os.path.join(self.root_directory, '{0}.raw_corpus.txt'.format(self.label))

    @property
    def metadata_file(self):
        return os.path.join(self.root_directory, '{0}.metadata.csv'.format(self.label))

    def preprocessed_corpus_file(self, normalization_options):
        normalization_label = normalization.filenameLabel(normalization_options)
        return os.path.join(self.root_directory, '{0}.preprocessed_corpus{1}.txt'.format(
            self.label,
            ('' if len(normalization_label) == 0 else '.{0}'.format(normalization_label))
        ))

    def tagged_corpus_file(self, normalization_options, terminology):
        normalization_label = normalization.filenameLabel(normalization_options)
        return os.path.join(self.root_directory, '{0}.tagged_corpus.{1}{2}.txt'.format(
            self.label,
            terminology,
            ('' if len(normalization_label) == 0 else '.{0}'.format(normalization_label))
        ))

class LiteratureSnapshotCorpusDocument:
    ID = None
    source = None
    included_abstract = False
    included_full_text = False
    doc_obj = None

    def __init__(self, ID, source, included_abstract, included_full_text, doc_obj=None):
        self.ID = ID
        self.source = source
        self.included_abstract = included_abstract
        self.included_full_text = included_full_text
        self.doc_obj = doc_obj

    def __eq__(self, other):
        return (
            (self.ID == other.ID)
            and (self.source == other.source)
            and (self.included_abstract == other.included_abstract)
            and (self.included_full_text == other.included_full_text)
        )
