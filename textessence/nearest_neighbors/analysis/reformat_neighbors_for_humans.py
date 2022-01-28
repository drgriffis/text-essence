'''
Convert a neighbors file to human-readable format,
optionally including preferred string expansion.
'''

from hedgepig_logger import log
from .. import nn_io

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-i', '--input', dest='inputf',
            help='(REQUIRED) input neighbors file')
        parser.add_option('-o', '--output', dest='outputf',
            help='(REQUIRED) output remapped neighbors file')
        parser.add_option('-v', '--vocab', dest='vocabf',
            help='(REQUIRED) neighbor ID <-> key mapping file')
        parser.add_option('-k', '--nearest-neighbors', dest='k',
            help='number of nearest neighbors to use in statistics (default: %default)',
            type='int', default=5)
        parser.add_option('-m', '--string-map', dest='string_mapf',
            help='file mapping embedding keys to strings')
        parser.add_option('--with-distances', dest='with_distances',
            action='store_true', default=False,
            help='neighbor files have distance information')
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.inputf:
            parser.error('Must provide --input')
        elif not options.outputf:
            parser.error('Must provide --output')
        elif not options.vocabf:
            parser.error('Must provide --vocab')
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Input neighbors file', options.inputf),
        ('Remapped neighbors file', options.outputf),
        ('Number of nearest neighbors to pull', options.k),
        ('String map file', options.string_mapf),
        ('Vocab file', options.vocabf),
        ('Using distance information', options.with_distances),
    ], 'Neighborhood file remapping')

    node_map = nn_io.readNodeMap(options.vocabf)

    neighbors = nn_io.readNeighborFile(
        options.inputf,
        k=options.k,
        node_map=node_map,
        with_distances=options.with_distances
    )

    if options.string_mapf:
        log.writeln('Reading string map from %s...' % options.string_mapf)
        string_map = nn_io.readStringMap(options.string_mapf, lower_keys=True)
        log.writeln('Mapped strings for {0:,} keys.\n'.format(len(string_map)))

        remap_key = lambda key: '%s (%s)' % (key, string_map.get(key, '-UNKNOWN-'))
    else:
        string_map = None
        remap_key = lambda key: key

    log.writeln('Writing remapped neighbor info to %s...' % options.outputf)
    log.track('  >> Wrote {0:,} neighbor sets', writeInterval=100)
    with open(options.outputf, 'w') as stream:
        for (key, nbrs) in neighbors.items():
            if options.with_distances:
                nbrs = [
                    (remap_key(k), dist)
                        for (k,dist) in nbrs
                ]
            else:
                nbrs = [remap_key(k) for k in nbrs]

            stream.write('--------------------------------\n')
            stream.write('{0}\n'.format(remap_key(key)))
            for nbr_info in nbrs:
                if options.with_distances:
                    stream.write('  {0} --> {1}\n'.format(
                        remap_key(nbr_info[0]),
                        nbr_info[1]
                    ))
                else:
                    stream.write('  {0}\n'.format(remap_key(nbr_info)))
            #nn_io.writeNeighborFileLine(
            #    stream,
            #    remap_key(key),
            #    nbrs,
            #    with_distances=options.with_distances
            #)
            log.tick()
    log.flushTracker()

    log.stop()
