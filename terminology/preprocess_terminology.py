import scispacy
import spacy
from hedgepig_logger import log
from lib import normalization

def readAndNormalizeTerminology(f, options):
    log.track(message='  >> Processed {0:,} terminology entries', writeInterval=100)
    normalizer = normalization.Normalizer(options)
    terminology = {}
    with open(f, 'r') as stream:
        nlp = spacy.load('en_core_sci_lg')
        for line in stream:
            try:
                (code, term) = [s.strip() for s in line.split('\t')]
                tokens = nlp(term)
                normalized_term = ' '.join(normalizer.normalize(tokens))

                if not code in terminology:
                    terminology[code] = set()
                terminology[code].add(normalized_term)
            except ValueError:
                log.writeln('\n[WARNING] Failed to parse line "%s"' % line)
            log.tick()
    log.flushTracker()
    return terminology

def writeTerminology(terminology, f):
    with open(f, 'w') as stream:
        for (key, terms) in terminology.items():
            for term in terms:
                stream.write('%s\t%s\n' % (key, term))

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-i', '--input', dest='input_f',
            help='(required) unnormalized terminology file')
        parser.add_option('-o', '--output', dest='output_f',
            help='(required) normalized output terminology file')
        normalization.CLI.addNormalizationOptions(parser)
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.input_f:
            parser.print_help()
            parser.error('Must provide --input')
        if not options.output_f:
            parser.print_help()
            parser.error('Must provide --output')
        return options
    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Input (unnormalized) terminology file', options.input_f),
        ('Output (normalized) terminology file', options.output_f),
        ('Normalization options', normalization.CLI.logNormalizationOptions(options)),
    ], 'Terminology preprocessing')

    log.writeln('Reading and normalizing terminology from %s...' % options.input_f)
    terminology = readAndNormalizeTerminology(options.input_f, options)
    log.writeln('Terminology contains {0:,} entries after normalization.\n'.format(
        sum([len(v) for (k,v) in terminology.items()])
    ))

    log.writeln('Writing normalized terminology to %s...' % options.output_f)
    writeTerminology(terminology, options.output_f)
    log.writeln('Done.\n')

    log.stop()
