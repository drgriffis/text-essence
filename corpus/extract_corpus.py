import os
import csv
from hedgepig_logger import log
from corpus.data_processor import CORD19Deltas

def extractRecord(record, dataset, stream, abstract_only=False):
    status = {}

    ### record processing workflow
    # (1) pull the abstract from the metadata, as a single paragraph
    abstract = record['abstract'].strip()
    if len(abstract) > 0:
        stream.write('%s\n' % record['abstract'].strip())
        status['abstract'] = 1
    else:
        status['abstract'] = 0

    # (2) if this record has PMC JSON content, prefer it
    if len(record['pmc_json_files']) > 0:
        jsonpath = record['pmc_json_files']
    # (3) otherwise, check if it has PDF JSON content
    elif len(record['pdf_json_files']) > 0:
        jsonpath = record['pdf_json_files']
    # (4) otherwise, mark as no full-text
    else:
        jsonpath = None

    # (5) now, go through each JSON file to pull the full text
    status['full_text'] = 0
    if (not abstract_only) and jsonpath:
        jsonpaths = jsonpath.split(';')
        for jsonpath in jsonpaths:
            data = dataset.getJSON(jsonpath)
            if 'body_text' in data:
                paragraphs = data['body_text']
                for paragraph in paragraphs:
                    stream.write('%s\n' % paragraph['text'].strip())
                    status['full_text'] = 1
    
    return status
                
            

def extractCorpus(datadir, outf, new_format, abstract_only, refdir=None):
    status = {'Abstract only': 0, 'Full text only': 0, 'Both': 0, 'Omitted': 0}
    render_status = lambda status: ' '.join([
        '{0}: {1:,}'.format(k, v)
            for (k,v) in sorted(status.items())
    ])

    with open(outf, 'w') as stream, \
         open('%s.info' % outf, 'w') as info_stream:
        info_writer = csv.DictWriter(info_stream, fieldnames=['CORD_UID', 'Status'])
        info_writer.writeheader()

        t = log.startTimer('Loading CORD-19 dataset from %s...' % datadir)
        with CORD19Deltas(datadir, refdir, new_format=new_format) as dataset:
            log.stopTimer(t, 'Dataset loaded in {0:.2f}s: includes %s records in total.\n' % ('{0:,}'.format(len(dataset))))

            log.track('  >> Processed {0:,} new records (Status -- {1})', writeInterval=1)
            for record in dataset:
                record_status = extractRecord(record, dataset, stream, abstract_only=abstract_only)
                if record_status['abstract'] == 1 and record_status['full_text'] == 1:
                    stat = 'Both'
                elif record_status['abstract'] == 1:
                    stat = 'Abstract only'
                elif record_status['full_text'] == 1:
                    stat = 'Full text only'
                else:
                    stat = 'Omitted'
                status[stat] += 1
                if stat != 'Omitted':
                    info_writer.writerow({'CORD_UID': record['cord_uid'], 'Status': stat})
                log.tick(render_status(status))
            log.flushTracker(render_status(status))

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-d', '--directory', dest='directory',
            help='(required) CORD-19 output dump data directory')
        parser.add_option('-o', '--output', dest='output_f',
            help='(required) output file for corpus')
        parser.add_option('--new-format', dest='new_format',
            default=False, action='store_true',
            help='use if data directory is after 5/12/20')
        parser.add_option('--abstract-only', dest='abstract_only',
            default=False, action='store_true',
            help='do not extract full text data')
        parser.add_option('-r', '--reference-directory', dest='ref_directory',
            help='(optional) reference directory for pulling deltas only')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.directory:
            parser.print_help()
            parser.error('Must provide --directory')
        if not options.output_f:
            parser.print_help()
            parser.error('Must provide --output')
        return options
    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Input data directory', options.directory),
        ('Reference data directory for deltas', options.ref_directory),
        ('Output corpus file', options.output_f),
        ('Input data in new format?', options.new_format),
        ('Extracting abstracts only?', options.abstract_only),
    ], 'Extracting CORD-19 corpus')

    extractCorpus(
        options.directory, 
        options.output_f, 
        options.new_format, 
        options.abstract_only,
        refdir=options.ref_directory,
    )
    log.writeln()
    log.writeln('Extracted corpus file to %s' % options.output_f)

    log.stop()
