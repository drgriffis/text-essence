'''
Split a large preprocessed corpus file for parallel tagging jobs
'''

from hedgepig_logger import log

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-i', '--input', dest='input_f',
            help='(required) input corpus file to split')
        parser.add_option('-c', '--chunk-size', dest='chunk_size',
            type='int', default=100000,
            help='(>=2) number of lines to include in each chunk'
                 ' (default %default)')
        parser.add_option('--overlap', dest='overlap_size',
            type='int', default=10000,
            help='(< --chunk-size) number of lines to overlap between chunks'
                 ' (default %default)')
        parser.add_option('-o', '--output', dest='output_f',
            help='(required) base path for output files')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.input_f:
            parser.print_help()
            parser.error('Must provide --input')
        if options.chunk_size < 2:
            parser.print_help()
            parser.error('--chunk-size must be at least 2')
        if options.overlap_size >= options.chunk_size or options.overlap_size < 1:
            parser.print_help()
            parser.error('--overlap must be greater than 0 and less than --chunk-size')
        if not options.output_f:
            parser.print_help()
            parser.error('Must provide --output')
        return options
    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Input corpus file', options.input_f),
        ('Number of lines per output chunk', options.chunk_size),
        ('Number of overlap lines between chunks', options.overlap_size),
        ('Output chunk file base path', options.output_f),
    ], 'Large corpus splitting for parallel JET tagging')

    cur_chunk = 0
    cur_line = 0
    primary_stream = open('%s.chunk-%d' % (options.output_f, cur_chunk), 'w')
    overlap_stream = None

    log.track('  >> Processed {0:,} lines (on chunk {1:,})', writeInterval=1000)
    with open(options.input_f, 'r') as stream:
        for line in stream:
            if cur_line == options.chunk_size:
                assert overlap_stream is None
                overlap_stream = open('%s.chunk-%d' % (options.output_f, cur_chunk + 1), 'w')
            if cur_line == options.chunk_size + options.overlap_size:
                primary_stream.close()
                primary_stream = overlap_stream
                overlap_stream = None
                cur_chunk += 1
                cur_line = options.overlap_size
            primary_stream.write(line)
            if overlap_stream:
                overlap_stream.write(line)
            cur_line += 1
            log.tick(cur_chunk)
    log.flushTracker(cur_chunk)

    primary_stream.close()
    if overlap_stream:
        overlap_stream.close()

    log.stop()
