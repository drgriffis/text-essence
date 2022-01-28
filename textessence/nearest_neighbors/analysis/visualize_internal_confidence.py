import configparser
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
from hedgepig_logger import log
from ..database import *

def graphConfidenceDistributions(sources, k, db, outf):
    data, xlbls = [], []
    for source in sources:
        distrib = []
        confidences = db.selectFromInternalConfidence(
            src=source,
            at_k=k
        )
        for c in confidences:
            distrib.append(c.confidence)
        data.append(distrib)
        xlbls.append(source)

    (fig, ax) = plt.subplots(figsize=(9,3))
    indices = np.arange(len(xlbls)) + 1
    extra_artists = []

    violas = ax.violinplot(
        data,
        showmeans=True
    )
    for viola in violas['bodies']:
        viola.set_zorder(10)
        viola.set_alpha(0.8)

    ax.set_xticks(indices)
    ax.set_xticklabels(
        xlbls,
        rotation=60
    )

    plt.savefig(outf, extra_artists=extra_artists, bbox_inches='tight')
    plt.close()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-o', '--output', dest='outf',
            help='(required) output image file')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-k', '--nearest-neighbors', dest='k',
            help='number of nearest neighbors to use in statistics (default: %default)',
            type='int', default=5)
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()
        if not options.outf:
            parser.print_help()
            parser.error('Must provide --output')
        return options

    options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Configuration file', options.configf),
        ('Number of nearest neighbors to analyze', options.k),
        ('Output file', options.outf),
    ], 'Internal confidence distributions visualization')

    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    config = config['PairedNeighborhoodAnalysis']
    log.writeln('Done.\n')

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(config['DatabaseFile'])
    log.writeln('Database ready.\n')

    sources = [
        '2020-03-20',
        '2020-03-27',
        '2020-04-10',
        '2020-04-17',
        '2020-04-24',
        '2020-05-01'
    ]

    log.writeln('Generating graph for sources:')
    for s in sources:
        log.writeln('  {0}'.format(s))

    graphConfidenceDistributions(
        sources,
        options.k,
        db,
        options.outf
    )

    log.writeln('Visualization saved to {0}.\n'.format(options.outf))

    log.stop()
