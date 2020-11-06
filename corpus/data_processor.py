'''
'''

import os
import tarfile
import json
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
        self._keys = []
        self._file_counts = {}
        self._file_paths = {}

        FILES = NEW_FILES if new_format else OLD_FILES

        for f in FILES:
            fpath = os.path.join(data_dir, f)
            if os.path.exists(fpath):
                with tarfile.open(fpath, 'r') as tar:
                    num_files = len(tar.getmembers())
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
        self._current_files = list(self._current_tar.getmembers())
        self._current_file_ix = 0

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
