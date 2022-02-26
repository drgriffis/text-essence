import os
import csv
from datetime import datetime
from hedgepig_logger import log
from .cord19_data_models import CORD19Deltas, CORD19Format
from textessence.core.models.snapshot_data_models import *
from . import publication_date_resolution
from ..text_corpus_source_extractor_interface import TextCorpusSourceExtractorInterface
                

class CORD19DumpExtractor(TextCorpusSourceExtractorInterface):
    input_dump = None
    reference_dump = None
    
    def __init__(self, sources_configuration, source_name):
        super().__init__(sources_configuration, source_name)

        self.input_dump = self.sources_configuration[source_name]
        reference_dump = self.input_dump.get('ReferenceDump', None)
        if reference_dump:
            self.reference_dump = self.sources_configuration[reference_dump]

    def listConfigurationSettings(self):
        return [
            ('Input data dump', list(self.input_dump.items())),
            ('Reference data dump for deltas', (
                '-- NONE --' if (not self.reference_dump) else list(self.reference_dump.items())
            )),
        ]
            
    def extractDocuments(self, snapshot_collection):
        status = {'Abstracts': 0, 'Full texts': 0, 'Both': 0, 'Neither': 0}
        renderStatus = lambda status: ' '.join([
            '{0}: {1:,}'.format(k, v)
                for (k,v) in sorted(status.items())
        ])

        snapshots_touched = {}
        snapshot_touch_counts = {}
        date_resolution_counts = {}

        input_dir = self.input_dump['DistribDirectory']
        input_fmt = CORD19Format.parse(self.input_dump['Format'])
        input_date = self.input_dump['DumpDate']
        reference_dir = (None if self.reference_dump is None else self.reference_dump['DistribDirectory'])

        t = log.startTimer('Loading CORD-19 dataset from %s...' % input_dir)
        with CORD19Deltas(input_dir, reference_dir, data_format=input_fmt) as dataset:
            log.stopTimer(t, 'Dataset loaded in {0:.2f}s: includes %s records in total.\n' % ('{0:,}'.format(len(dataset))))

            log.track('  >> Processed {0:,} new records (Status -- {1})', writeInterval=1)
            for record in dataset:
                abstract, full_text = record.getAbstractAndFullText()

                has_abstract = ((not abstract is None) and len(abstract) > 0)
                has_full_text = ((not full_text is None) and len(full_text) > 0)

                # (1) check if there's anything to do with this record (i.e.,
                # either an abstract or a full text is present)
                if has_abstract or has_full_text:
                    # (2) if there is, figure out which month/year snapshot it
                    # belongs to
                    publication_date = record['publish_time']
                    snapshot_date, date_resolution_status = \
                        publication_date_resolution.getSnapshotDate(
                            publication_date, input_date
                        )
                    date_resolution_counts[date_resolution_status] = \
                        date_resolution_counts.get(date_resolution_status, 0) + 1

                    if snapshot_date is None:
                        continue
                    if not snapshot_date in snapshot_collection:
                        snapshot_collection.createSnapshot(snapshot_date)
                    snapshot = snapshot_collection[snapshot_date]

                    #   mark that we've touched this snapshot, so we can save
                    #   its updated metadata later
                    snapshots_touched[snapshot_date] = snapshot
                    snapshot_touch_counts[snapshot_date] = snapshot_touch_counts.get(snapshot_date, 0) + 1

                    # (3) add this document to the snapshot
                    document = LiteratureSnapshotDocument(
                        ID=record['cord_uid'],
                        dump_date=input_date,
                        publication_date=publication_date,
                        date_resolution_status=date_resolution_status,
                        has_abstract=has_abstract,
                        has_full_text=has_full_text
                    )
                    snapshot.addDocument(
                        document,
                        flush=False
                    )

                    # (4) and write out abstract/full text files as needed
                    has_both = True
                    if abstract and len(abstract) > 0:
                        with open(document.abstract_file, 'w') as stream:
                            stream.write(abstract.strip())
                            stream.write('\n')
                        status['Abstracts'] += 1
                    else:
                        has_both = False

                    if full_text and len(full_text) > 0:
                        with open(document.full_text_file, 'w') as stream:
                            for paragraph in full_text:
                                stream.write(paragraph.strip())
                                stream.write('\n')
                        status['Full texts'] += 1
                    else:
                        has_both = False

                    if has_both:
                        status['Both'] += 1
                else:
                    status['Neither'] += 1

                log.tick(renderStatus(status))
            log.flushTracker(renderStatus(status))

            # report out on distribution of extracted documents
            extraction_report = CORD19ExtractionReport(self.input_dump['RootDirectory'])
            extraction_report.extraction_timestamp = datetime.now()
            extraction_report.input_date = input_date
            extraction_report.input_directory = input_dir
            extraction_report.input_format = input_fmt
            extraction_report.reference_directory = reference_dir
            extraction_report.identified_records = len(dataset)
            extraction_report.number_with_abstract = status['Abstracts']
            extraction_report.number_with_full_text = status['Full texts']
            extraction_report.number_with_both = status['Both']
            extraction_report.snapshot_touch_counts = snapshot_touch_counts
            extraction_report.date_resolution_counts = date_resolution_counts

            extraction_report.write()
            extraction_report.renderToLog()

        # go through all the snapshots that we added documents to and flush
        # their updated metadata
        for snapshot in snapshots_touched.values():
            snapshot.writeMetadata()

class CORD19ExtractionReport:
    extraction_timestamp = None
    input_date = None
    input_directory = None
    input_format = None
    reference_directory = None

    identified_records = None
    number_with_abstract = None
    number_with_full_text = None
    number_with_both = None

    snapshot_touch_counts = None
    date_resolution_counts = None

    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S %z'
    TIMESTAMP_FORMAT_NO_TZ = '%Y-%m-%d %H:%M:%S'

    def __init__(self, root_dir):
        self._fpath = os.path.join(root_dir, 'extraction_report.csv')
        self.read()

    def read(self):
        if os.path.isfile(self._fpath):
            self.snapshot_touch_counts = {}
            self.date_resolution_counts = {}
            with open(self._fpath, 'r') as stream:
                reader = csv.reader(stream)
                for record in reader:
                    if record[0] == 'Extraction Timestamp':
                        try:
                            self.extraction_timestamp = datetime.strptime(
                                record[1],
                                CORD19ExtractionReport.TIMESTAMP_FORMAT
                            )
                        except ValueError:
                            self.extraction_timestamp = datetime.strptime(
                                record[1].strip(),
                                CORD19ExtractionReport.TIMESTAMP_FORMAT_NO_TZ
                            )
                    elif record[0] == 'Input Date':
                        self.input_date = record[1]
                    elif record[0] == 'Input Directory':
                        self.input_directory = record[1]
                    elif record[0] == 'Reference Directory':
                        self.reference_directory = record[1]
                    elif record[0] == 'Identified Records':
                        self.identified_records = int(record[1])
                    elif record[0] == 'Number with Abstract':
                        self.number_with_abstract = int(record[1])
                    elif record[0] == 'Number with Full Text':
                        self.number_with_full_text = int(record[1])
                    elif record[0] == 'Number with Both':
                        self.number_with_both = int(record[1])
                    elif record[0].split()[0] == 'Snapshot':
                        snapshot_key = record[0].split('-')[1]
                        self.snapshot_touch_counts[snapshot_key] = int(record[1])
                    elif record[0].split()[0] == 'Date':
                        resolution_type = record[0].split('-')[1]
                        self.date_resolution_counts[resolution_type] = int(record[1])

    def write(self):
        with open(self._fpath, 'w') as stream:
            writer = csv.writer(stream)
            for row in [
                (
                    'Extraction Timestamp',
                    self.extraction_timestamp.strftime(CORD19ExtractionReport.TIMESTAMP_FORMAT)
                ),
                ('Input Date', self.input_date),
                ('Input Directory', self.input_directory),
                ('Reference Directory', self.reference_directory),
                ('Identified Records', self.identified_records),
                ('Number with Abstract', self.number_with_abstract),
                ('Number with Full Text', self.number_with_full_text),
                ('Number with Both', self.number_with_both),
                *[
                    ('Snapshot Touch Count - {0}'.format(snapshot_key), count)
                        for (snapshot_key, count) in sorted(self.snapshot_touch_counts.items())
                ],
                *[
                    ('Date Resolution Count - {0}'.format(resolution_type), count)
                        for (resolution_type, count) in sorted(self.date_resolution_counts.items())
                ]
            ]:
                writer.writerow(row)

    def renderToLog(self):
        log.writeln()
        log.writeln('Extraction report written to:')
        log.writeln('  {0}'.format(self._fpath))
        log.writeln()
        log.writeln('== SNAPSHOT TOUCH COUNT REPORT ==')
        for (snapshot, cnt) in sorted(self.snapshot_touch_counts.items()):
            log.writeln('{0} --> {1:,} entries'.format(snapshot, cnt))

        log.writeln()
        log.writeln('== PUBLICATION DATE RESOLUTION REPORT ==')
        for (res_status, cnt) in sorted(self.date_resolution_counts.items()):
            log.writeln('{0} --> {1:,} entries'.format(res_status, cnt))
