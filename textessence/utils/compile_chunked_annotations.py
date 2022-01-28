'''
Unify JET annotation files from a corpus chunked up with split_corpus_for_tagging
'''

from hedgepig_logger import log

def readAnnotations(f):
    annotations = []
    log.track('  >> Read {0:,} lines', writeInterval=100000)
    with open(f, 'r') as stream:
        for line in stream:
            annotations.append(line.strip())
            log.tick()
    log.flushTracker()
    return annotations

def chunkMatch(c1, c2):
    valid = True
    if c1[0].split()[1:] == c2[0].split()[1:]:
        for k in range(len(c1)-1):
            if c1[k+1] != c2[k+1]:
                valid = False
                break
    else:
        valid = False
    return valid

def alignAnnotations(chunk_1, chunk_2):
    initial_chunk_size = 5
    match_start = chunk_2[:initial_chunk_size]
    log.indent()
    log.track('  >> Scanned {0:,} lines', writeInterval=10000)
    for i in range(len(chunk_1)):
        if chunkMatch(
            chunk_1[i:i+initial_chunk_size],
            match_start
        ):
            log.writeln()
            log.writeln('Matched initial chunk @ Line {0:,}'.format(i))
            j = 0
            while (
                (initial_chunk_size + j < len(chunk_2))
                and (i+initial_chunk_size+j < len(chunk_1))
                and (chunk_1[i+initial_chunk_size+j] == chunk_2[initial_chunk_size+j])
            ):
                j += 1
            log.writeln('Chunk match terminated after {0:,} lines'.format(initial_chunk_size+j))
            log.writeln('  Chunk 1 index: {0:,}/{1:,}'.format(
                i+initial_chunk_size+j, len(chunk_1)
            ))
            log.writeln('  Chunk 2 index: {0:,}/{1:,}'.format(
                initial_chunk_size+j, len(chunk_2)
            ))

            if (i+initial_chunk_size+j) == len(chunk_1):
                if chunkMatch(
                    chunk_1[i:],
                    chunk_2[:initial_chunk_size+j]
                ):
                    return i
                else:
                    raise Exception('Failed to match overlap chunks')
        log.tick()
    log.flushTracker()
    log.unindent()

    return None

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog ANNOT_FILE_1 ANNOT_FILE_2 [ANNOT_FILE_3 [...]]')
        parser.add_option('-o', '--output', dest='output_f',
            help='(required) output file to write compiled annotations to')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='file to write logging messages to')
        (options, args) = parser.parse_args()
        if not options.output_f:
            parser.print_help()
            parser.error('Must provide --output')
        if len(args) < 2:
            parser.print_help()
            parser.error('Must provide at least two chunked annotation files')
        options.chunk_files = args
        return options
    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Chunk files', [
            ('File {0:,}'.format(i), options.chunk_files[i])
                for i in range(len(options.chunk_files))
        ]),
        ('Output file', options.output_f),
    ], 'Compiling chunked JET corpus annotations')

    log.writeln('Reading annotations from chunk 0...')
    current_chunk = readAnnotations(options.chunk_files[0])
    log.writeln('Read {0:,} annotations.\n'.format(len(current_chunk)))

    with open(options.output_f, 'w') as stream:
        for i in range(1, len(options.chunk_files)):
            log.writeln('Reading annotations from chunk {0:,}...'.format(i))
            next_chunk = readAnnotations(options.chunk_files[i])
            log.writeln('Read {0:,} annotations.\n'.format(len(next_chunk)))

            log.writeln('Aligning annotations...')
            align_index = alignAnnotations(current_chunk, next_chunk)
            log.writeln('Align index: {0:,}\n'.format(align_index))

            # always take the first overlapping line from current_chunk, because
            # its word offset is correct (next_chunk may have the wrong offset for
            # the first item due to chunking at line breaks)
            for line in current_chunk[:align_index+1]:
                stream.write('%s\n' % line)
            log.writeln('Wrote first {0:,} lines of current chunk to output.\n'.format(align_index))
            current_chunk = next_chunk[1:]

        for line in current_chunk:
            stream.write('%s\n' % line)
        log.writeln('Wrote last {0:,} lines of final chunk to output.\n'.format(len(current_chunk)))

    log.stop()
