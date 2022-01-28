'''
'''

import sys
import os
import tarfile
import json
import csv
from collections import OrderedDict
from hedgepig_logger import log

## for file distributions prior to 5/12/20
SPLIT_FILES = [
    'comm_use_subset.tar.gz',
    'noncomm_use_subset.tar.gz',
    'custom_license.tar.gz',
    'biorxiv_medrxiv.tar.gz'
]
## for file distributions on or after 5/12/20
UNIFIED_FILES = [
    'document_parses.tar.gz'
]

class CORD19Format:
    Split_PDF_Only = 1
    Split_PDF_And_PMC = 2
    Unified = 3

    @staticmethod
    def parse(key):
        if key.lower() == 'split_pdf_only':
            return CORD19Format.Split_PDF_Only
        elif key.lower() == 'split_pdf_and_pmc':
            return CORD19Format.Split_PDF_And_PMC
        elif key.lower() == 'unified':
            return CORD19Format.Unified
        else:
            raise KeyError(key)

    @staticmethod
    def options():
        return [
            'Split_PDF_Only',
            'Split_PDF_And_PMC',
            'Unified'
        ]

class CORD19Dataset:
    
    def __init__(self, data_dir, data_format=CORD19Format.Unified):
        self._data_dir = data_dir
        self._data_format = data_format

        self._keys = []
        self._file_counts = {}
        self._file_paths = {}

        FILES = UNIFIED_FILES if (data_format == CORD19Format.Unified) else SPLIT_FILES

        for f in FILES:
            fpath = os.path.join(data_dir, f)
            if os.path.exists(fpath):
                self._file_paths[f] = fpath

        self._len = -1

    def __enter__(self):
        self._tar_streams = {
            key.replace('.tar.gz', ''): tarfile.open(path)
                for (key, path) in self._file_paths.items()
        }
        self._len = 0
        for stream in self._tar_streams.values():
            self._len += len(stream.getnames())
        return self

    def __exit__(self, type, value, traceback):
        for stream in self._tar_streams.values():
            stream.close()
        self._tar_streams = None

    def __len__(self):
        return self._len

    def __iter__(self):
        return self

    def getJSON(self, key):
        '''
        NOTE if using the Split_PDF_And_PMC format,
        MAKE SURE TO UNZIP THE .tar.gz files (tar files do not
        correctly index into the contents)
        '''
        if self._data_format == CORD19Format.Split_PDF_Only:
            tarkey = key.split('/')[0]
            tarinfo = self._tar_streams[tarkey].getmember(key.strip())
            tarfile = self._tar_streams[tarkey].extractfile(tarinfo)
            data = json.loads(tarfile.read())
        elif self._data_format == CORD19Format.Split_PDF_And_PMC:
            if not os.path.exists(key):
                sys.stderr.write('[ERROR] Attempting to load "{0}", file not found.\n'.format(key))
                sys.stderr.write('[ERROR] Have you unzipped the .tar.gz files in this distribution?\n')
                sys.stderr.write('[ERROR] This is REQUIRED if using CORD19Format Split_PDF_And_PMC.')
                sys.stderr.write('[ERROR] Allowing exception to throw, for traceback...')
            with open(key, 'r') as stream:
                data = json.loads(stream.read())
        else:
            tarinfo = self._tar_streams['document_parses'].getmember(key.strip())
            tarfile = self._tar_streams['document_parses'].extractfile(tarinfo)
            data = json.loads(tarfile.read())
        return data


class CORD19Deltas(CORD19Dataset):
    
    def __init__(self, data_dir, ref_dir, data_format=CORD19Format.Unified):
        super().__init__(data_dir, data_format=data_format)

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
            return CORD19Record(record, self)


class CORD19Record:
    def __init__(self, record, dataset):
        self._record = record
        self._dataset = dataset

    def __getitem__(self, key):
        return self._record[key]

    def getAbstractAndFullText(self, abstract_only=False):
        ### record processing workflow
        # (1) pull the abstract from the metadata, as a single paragraph
        abstract = self._record['abstract'].strip()

        if self._dataset._data_format == CORD19Format.Unified:
            # (2) if this record has PMC JSON content, prefer it
            if len(self._record['pmc_json_files']) > 0:
                jsonpath = self._record['pmc_json_files']
            # (3) otherwise, check if it has PDF JSON content
            elif len(self._record['pdf_json_files']) > 0:
                jsonpath = self._record['pdf_json_files']
            # (4) otherwise, mark as no full-text
            else:
                jsonpath = None

        elif self._dataset._data_format == CORD19Format.Split_PDF_And_PMC:
            # (2) if this record has PMC JSON content, prefer it
            if self._record['has_pmc_xml_parse'] == 'True' and len(self._record['full_text_file']) > 0:
                jsonpath = os.path.join(
                    self._dataset._data_dir,
                    self._record['full_text_file'],
                    'pmc_json',
                    '%s.xml.json' % self._record['pmcid']
                )
            # (3) otherwise, check if it has PDF JSON content
            elif self._record['has_pdf_parse'] == 'True' and len(self._record['full_text_file']) > 0:
                jsonpath = os.path.join(
                    self._dataset._data_dir,
                    self._record['full_text_file'],
                    'pdf_json',
                    '%s.json' % (self._record['sha'].split(';')[0])
                )
            # (4) otherwise, mark as no full-text
            else:
                jsonpath = None

        elif self._dataset._data_format == CORD19Format.Split_PDF_Only:
            # (2) -- no PMC JSON content in these versions --
            # (3) check for PDF JSON content
            if self._record['has_full_text'] == 'True' and len(self._record['full_text_file']) > 0:
                jsonpath = '%s/%s.json' % (
                    self._record['full_text_file'],
                    self._record['sha'].split(';')[0]
                )
            # (4) otherwise, mark as no full-text
            else:
                jsonpath = None

        # (5) now, go through each JSON file to pull the full text
        full_text = []
        if (not abstract_only) and jsonpath:
            jsonpaths = jsonpath.split(';')
            for jsonpath in jsonpaths:
                data = self._dataset.getJSON(jsonpath)
                if 'body_text' in data:
                    paragraphs = data['body_text']
                    for paragraph in paragraphs:
                        full_text.append(paragraph['text'].strip())

        return (abstract, full_text)
