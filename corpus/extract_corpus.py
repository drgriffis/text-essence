import os
import csv
from hedgepig_logger import log
from corpus.data_processor import CORD19Deltas, Format

def extractRecord(record, dataset, stream, abstract_only=False):
    status = {}

    abstract, full_text = record.getAbstractAndFullText(
        abstract_only=abstract_only
    )

    # write the abstract as a single paragraph
    if len(abstract) > 0:
        stream.write('%s\n' % abstract)
        status['abstract'] = 1
    else:
        status['abstract'] = 0

    # write full text paragraph by paragraph
    status['full_text'] = 0
    if (not abstract_only) and len(full_text) > 0:
        for paragraph in full_text:
            stream.write('%s\n' % paragraph.strip())
            status['full_text'] = 1
    
    return status
                
            

def extractCorpus(datadir, outf, data_format, abstract_only, refdir=None):
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
        with CORD19Deltas(datadir, refdir, data_format=data_format) as dataset:
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
        parser.add_option('--format', dest='data_format',
            type='choice', choices=Format.options(),
            help='format of the data directory (choices: [%s])' % (
                ','.join(Format.options())
            ))
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
        ('Data format', options.data_format),
        ('Extracting abstracts only?', options.abstract_only),
    ], 'Extracting CORD-19 corpus')

    extractCorpus(
        options.directory, 
        options.output_f, 
        Format.parse(options.data_format), 
        options.abstract_only,
        refdir=options.ref_directory,
    )
    log.writeln()
    log.writeln('Extracted corpus file to %s' % options.output_f)

    log.stop()
