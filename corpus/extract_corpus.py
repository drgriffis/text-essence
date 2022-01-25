import os
import csv
import configparser
from hedgepig_logger import log
from corpus.data_processor import CORD19Deltas, Format
from .snapshot_data_models import *
from . import publication_date_resolution
                
            
def extractDocumentsFromDump(input_dump, snapshot_collection,
        reference_dump=None):
    status = {'Abstracts': 0, 'Full texts': 0, 'Neither': 0}
    renderStatus = lambda status: ' '.join([
        '{0}: {1:,}'.format(k, v)
            for (k,v) in sorted(status.items())
    ])

    snapshots_touched = {}
    snapshot_touch_counts = {}
    date_resolution_counts = {}

    input_dir = input_dump['DistribDirectory']
    input_fmt = Format.parse(input_dump['Format'])
    input_date = input_dump['DumpDate']
    reference_dir = (None if reference_dump is None else reference_dump['DistribDirectory'])

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
                if abstract and len(abstract) > 0:
                    with open(document.abstract_file, 'w') as stream:
                        stream.write(abstract.strip())
                        stream.write('\n')
                    status['Abstracts'] += 1
                if full_text and len(full_text) > 0:
                    with open(document.full_text_file, 'w') as stream:
                        for paragraph in full_text:
                            stream.write(paragraph.strip())
                            stream.write('\n')
                    status['Full texts'] += 1
            else:
                status['Neither'] += 1

            log.tick(renderStatus(status))
        log.flushTracker(renderStatus(status))

    # go through all the snapshots that we added documents to and flush
    # their updated metadata
    for snapshot in snapshots_touched.values():
        snapshot.writeMetadata()

    # and report out on distribution of extracted documents
    log.writeln()
    log.writeln('== SNAPSHOT TOUCH COUNT REPORT ==')
    for (snapshot, cnt) in sorted(snapshot_touch_counts.items()):
        log.writeln('{0} --> {1:,} entries'.format(snapshot, cnt))

    log.writeln()
    log.writeln('== PUBLICATION DATE RESOLUTION REPORT ==')
    for (res_status, cnt) in sorted(date_resolution_counts.items()):
        log.writeln('{0} --> {1:,} entries'.format(res_status, cnt))


if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-c', '--config', dest='config_f',
            help='(required) configuration file')
        parser.add_option('-d', '--dump', dest='dump',
            help='(required) name of data dump to extract from')
        (options, args) = parser.parse_args()
        if not options.config_f:
            parser.print_help()
            parser.error('Must provide --config')
        if not options.dump:
            parser.print_help()
            parser.error('Must provide --dump')
        return options
    options = _cli()

    config = configparser.ConfigParser()
    config.read(options.config_f)

    logfile = os.path.join(
        config['Logging']['CORDExtractionLogs'],
        '{0}_extraction.log'.format(options.dump)
    )

    input_dump = config[options.dump]
    output_root_dir = config['TemporalCorpora']['RootDirectory']

    reference_dump = config[options.dump].get('ReferenceDump', None)
    if reference_dump:
        reference_dump = config[reference_dump]

    log.start(logfile)
    log.writeConfig([
        ('Input data dump', list(input_dump.items())),
        ('Reference data dump for deltas', list(reference_dump.items())),
        ('Output root directory', output_root_dir),
    ], 'Extracting CORD-19 corpus')

    collection = LiteratureSnapshotCollection(
        output_root_dir
    )

    extractDocumentsFromDump(
        input_dump,
        collection,
        reference_dump=reference_dump
    )

    log.stop()
