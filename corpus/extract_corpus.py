from hedgepig_logger import log
from corpus.data_processor import CORD19Dataset

def extractRecord(record, stream, abstract_only=False):
    status = {}

    if abstract_only:
        KEYS = ['abstract']
    else:
        KEYS = ['abstract', 'body_text']

    for key in KEYS:
        try:
            paragraphs = record[key]
            for paragraph in paragraphs:
                stream.write('%s\n' % paragraph['text'].strip())
            status[key] = 0
        except KeyError:
            status[key] = 1
    return status
            

def extractCorpus(datadir, outf, new_format, abstract_only):
    errors = {'Omitted fully': 0}
    render_errors = lambda err: ' '.join([
        '{0}: {1:,}'.format(k, v)
            for (k,v) in err.items()
    ])
    with open(outf, 'w') as stream:
        log.writeln('Loading CORD-19 dataset from %s...' % datadir)
        with CORD19Dataset(datadir, new_format=new_format) as dataset:
            log.writeln('Dataset loaded: includes {0:,} records.\n'.format(len(dataset)))

            log.track('  >> Processed {0:,}/%s records (Errors -- {1})' % ('{0:,}'.format(len(dataset))), writeInterval=1)
            for record in dataset:
                record_errors = extractRecord(record, stream, abstract_only=abstract_only)
                for (k,v) in record_errors.items():
                    if v == 1: errors[k] = errors.get(k,0) + 1
                if sum(record_errors.values()) == len(record_errors):
                    errors['Omitted fully'] = errors['Omitted fully'] + 1
                log.tick(render_errors(errors))
    log.flushTracker(render_errors(errors))

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
        ('Output corpus file', options.output_f),
        ('Input data in new format?', options.new_format),
        ('Extracting abstracts only?', options.abstract_only),
    ], 'Extracting CORD-19 corpus')

    extractCorpus(options.directory, options.output_f, options.new_format, options.abstract_only)
    log.writeln()
    log.writeln('Extracted corpus file to %s' % options.output_f)

    log.stop()
