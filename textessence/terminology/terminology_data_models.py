import os
import csv
from textessence.lib import normalization

class TerminologyCollection:
    terminologies = None

    def __init__(self, root_directory):
        self.root_directory = root_directory
        self.terminologies = {}
        self._loadMetadata()

    def __contains__(self, key):
        return key in self.terminologies
    def __getitem__(self, key):
        return self.terminologies[key]
    def get(self, key, default=None):
        return self.terminologies.get(key, default)

    def addTerminology(self, key):
        if key in self.terminologies:
            raise KeyError('Terminology key "{0}" already exists'.format(key))

        terminology_root_dir = os.path.join(self.root_directory, key)
        if not os.path.exists(terminology_root_dir):
            os.mkdir(terminology_root_dir)

        terminology = Terminology(
            key,
            terminology_root_dir
        )

        self.terminologies[key] = terminology
        self._saveMetadata()

        return terminology

    @property
    def _metadata_path(self):
        return os.path.join(self.root_directory, 'terminologies_metadata.csv')
    def _loadMetadata(self):
        if os.path.exists(self._metadata_path):
            with open(self._metadata_path, 'r') as stream:
                reader = csv.DictReader(stream)
                for record in reader:
                    self.terminologies[record['TerminologyKey']] = Terminology(
                        record['TerminologyKey'],
                        record['RootDirectory']
                    )
    def _saveMetadata(self):
        with open(self._metadata_path, 'w') as stream:
            writer = csv.DictWriter(stream, fieldnames=[
                'TerminologyKey',
                'RootDirectory'
            ])
            writer.writeheader()
            for (terminology_key, terminology) in self.terminologies.items():
                writer.writerow({
                    'TerminologyKey': terminology_key,
                    'RootDirectory': terminology.root_directory
                })

class Terminology:
    label = None
    root_directory = None

    def __init__(self, label, root_directory):
        self.label = label
        self.root_directory = root_directory

    @property
    def raw_terminology_file(self):
        return os.path.join(self.root_directory, '{0}.raw_terminology.txt'.format(self.label))
    @property
    def filtered_terminology_file(self):
        return os.path.join(self.root_directoyr, '{0}.filtered_terminology.txt'.format(self.label))

    def preprocessed_dir(self, normalization_options):
        normalization_label = normalization.filenameLabel(normalization_options)
        return os.path.join(self.root_directory, 'preprocessed{0}'.format(
            ('' if len(normalization_label) == 0 else '.{0}'.format(normalization_label))
        ))

    def preprocessed_terminology_file(self, normalization_options):
        return os.path.join(
            self.preprocessed_dir(normalization_options),
            '{0}.preprocessed_terminology.txt'.format(self.label)
        )

class FlatTerminology:
    def __init__(self, filepath=None):
        self.filepath = filepath
        self._mapping = {}
        self.read()

    def addMapping(self, key, value):
        if not key in self._mapping:
            self._mapping[key] = set()
        self._mapping[key].add(value)

    def write(self):
        with open(self.filepath, 'w') as stream:
            writer = csv.writer(stream, delimiter='\t')
            for key in sorted(self._mapping.keys()):
                for value in self._mapping[key]:
                    writer.writerow([key, value])

    def read(self):
        if self.filepath and os.path.exists(self.filepath):
            with open(self.filepath, 'r') as stream:
                reader = csv.reader(stream, delimiter='\t')
                for record in reader:
                    (key, value) = record
                    self.addMapping(key, value)

    @property
    def num_terms(self):
        return sum([
            len(v)
                for (k,v) in self._mapping.items()
        ])

    def __len__(self):
        return len(self._mapping)
    def __iter__(self):
        return iter(self._mapping)
    def items(self):
        return self._mapping.items()
