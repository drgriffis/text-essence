'''
'''

import os
import tarfile
import json
import csv
from collections import OrderedDict
from hedgepig_logger import log

## for file distributions prior to 5/12/20
OLD_FILES = [
    'comm_use_subset.tar.gz',
    'noncomm_use_subset.tar.gz',
    'custom_license.tar.gz',
    'biorxiv_medrxiv.tar.gz'
]
## for file distributions on or after 5/12/20
NEW_FILES = [
    'document_parses.tar.gz'
]

class CORD19Dataset:
    
    def __init__(self, data_dir, new_format=True):
        self._data_dir = data_dir
        self._keys = []
        self._file_counts = {}
        self._file_paths = {}

        FILES = NEW_FILES if new_format else OLD_FILES

        for f in FILES:
            fpath = os.path.join(data_dir, f)
            if os.path.exists(fpath):
                with tarfile.open(fpath, 'r') as tar:
                    #num_files = len(tar.getmembers())
                    num_files = 1  ##HACK
                    if num_files > 0:
                        self._keys.append(f)
                        self._file_counts[f] = num_files
                        self._file_paths[f] = fpath

        self._current_key_ix = -1
        self._current_tar = None

    def __enter__(self):
        self.openNextTar()
        return self

    def __exit__(self, type, value, traceback):
        if self._current_tar:
            self._current_tar.close()

    def openNextTar(self):
        if self._current_tar:
            self._current_tar.close()

        self._current_key_ix += 1
        if self._current_key_ix >= len(self._keys):
            raise IndexError

        self._current_tar = tarfile.open(
            self._file_paths[self._keys[self._current_key_ix]],
            'r'
        )
        #self._current_files = list(self._current_tar.getmembers())
        #self._current_file_ix = 0

    def __next__(self):
        if self._current_file_ix >= len(self._current_files) - 1:
            try:
                self.openNextTar()
            except IndexError:
                raise StopIteration

        tarinfo = self._current_files[self._current_file_ix]
        if tarinfo.isdir():
            self._current_file_ix += 1
            return next(self)
        else:
            tarfile = self._current_tar.extractfile(tarinfo)
            data = json.loads(tarfile.read())

            self._current_file_ix += 1
            return data

    def __len__(self):
        return sum(self._file_counts.values())

    def __iter__(self):
        return self

    def getJSON(self, key):
        tarinfo = self._current_tar.getmember(key.strip())
        tarfile = self._current_tar.extractfile(tarinfo)
        data = json.loads(tarfile.read())
        return data


class CORD19Deltas(CORD19Dataset):
    
    def __init__(self, data_dir, ref_dir, new_format=True):
        super().__init__(data_dir, new_format=new_format)

        if ref_dir:
            log.indent()
            ref_metadata = os.path.join(ref_dir, 'metadata.csv')
            log.writeln('Reading set of reference paper IDs from %s...' % ref_metadata)
            ref_paper_IDs = set ()
            with open(ref_metadata, 'r') as stream:
                reader = csv.DictReader(stream)
                for record in reader:
                    ref_paper_IDs.add(record['cord_uid'])
            log.writeln('Found {0:,} paper IDs.'.format(len(ref_paper_IDs)))
            log.unindent()
        else:
            ref_paper_IDs = set()
        self._ref_paper_IDs = ref_paper_IDs

        self._metadata_stream = None

    def __exit__(self, type, value, traceback):
        if self._metadata_stream:
            self._metadata_stream.close()
        super().__exit__(type, value, traceback)

    def __iter__(self):
        metadata_f = os.path.join(self._data_dir, 'metadata.csv')
        self._metadata_stream = open(metadata_f, 'r')
        self._metadata_reader = csv.DictReader(self._metadata_stream)
        return self

    def __next__(self):
        searching, halt = True, False
        while searching:
            try:
                record = next(self._metadata_reader)
                paper_ID = record['cord_uid']
                if not paper_ID in self._ref_paper_IDs:
                    searching = False
            except StopIteration:
                self._metadata_stream.close()
                searching = False
                halt = True
        if halt:
            raise StopIteration
        else:
            return record
