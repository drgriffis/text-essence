from .terminology import SnomedTerminology
from hedgepig_logger import log

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('--concepts', dest='concepts_f',
            help='(required) SNOMED-CT concepts file')
        parser.add_option('--descriptions', dest='descriptions_f',
            help='(required) SNOMED-CT descriptions file')
        parser.add_option('--definitions', dest='definitions_f',
            help='(required) SNOMED-CT definitions file')
        parser.add_option('-o', '--output', dest='output_f',
            help='(required) output file for SNOMED-CT flat terminology')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.concepts_f:
            parser.print_help()
            parser.error('Must provide --concepts')
        if not options.descriptions_f:
            parser.print_help()
            parser.error('Must provide --descriptions')
        if not options.definitions_f:
            parser.print_help()
            parser.error('Must provide --definitions')
        if not options.output_f:
            parser.print_help()
            parser.error('Must provide --output')
        return options
    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Concepts file', options.concepts_f),
        ('Descriptions file', options.descriptions_f),
        ('Definitions file', options.definitions_f),
        ('Output file', options.output_f),
    ], 'SNOMED-CT terminology extraction')

    log.writeln('Loading SNOMED-CT terminology...')
    terminology = SnomedTerminology(
        concepts_file=options.concepts_f,
        descriptions_file=options.descriptions_f,
        definitions_file=options.definitions_f,
        verbose=True
    )
    log.writeln('Loaded {0:,} concepts.\n'.format(len(terminology)))

    log.writeln('Writing flat terminology to %s...' % options.output_f)
    log.track('  >> Wrote {0:,} concept-term mappings', writeInterval=100)
    with open(options.output_f, 'w') as stream:
        for concept in terminology:
            for description in concept.descriptions:
                stream.write('%s\t%s\n' % (
                    concept.ID,
                    description.term
                ))
                log.tick()
    log.flushTracker()

    log.stop()
