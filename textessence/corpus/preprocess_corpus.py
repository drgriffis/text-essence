import scispacy
import spacy
from hedgepig_logger import log
from lib import normalization

def preprocess(corpusf, outf, options):
    log.track(message='  >> Processed {0:,} paragraphs', writeInterval=100)
    normalizer = normalization.Normalizer(options)
    with open(corpusf, 'r') as in_stream, \
         open(outf, 'w') as out_stream:
        nlp = spacy.load('en_core_sci_lg')
        for line in in_stream:
            para = nlp(line)
            for sent in para.sents:
                tokens = normalizer.normalize(sent)
                out_stream.write('%s\n' % (' '.join(tokens)))
            log.tick()
    log.flushTracker()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-i', '--input', dest='input_f',
            help='(required) corpus file to preprocess')
        parser.add_option('-o', '--output', dest='output_f',
            help='(required) output file to write preprocessed data to')
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
        ('Input corpus file', options.input_f),
        ('Output corpus file', options.output_f),
        ('Normalization options', normalization.CLI.logNormalizationOptions(options)),
    ], 'CORD-19 corpus preprocessing')

    log.writeln('Preprocessing input corpus %s' % options.input_f)
    preprocess(
        options.input_f,
        options.output_f,
        options
    )
    log.writeln('Preprocessed corpus written to %s' % options.output_f)

    log.stop()
