import numpy as np
import matplotlib
matplotlib.use('agg')
from matplotlib import pyplot as plt

def entityChangeAnalysis(corpora, cwds, outf=None, figsize=(5,5), font_size=18):
    font = {
        'family' : 'sans-serif',
        'size'   : font_size
    }
    matplotlib.rc('font', **font)

    (fig, ax) = plt.subplots(figsize=figsize)

    x_ticks = np.arange(len(corpora)) + 1
    x_data, y_data = [], []
    for i in range(len(corpora)-1):
        if not (cwds[i] is None):
            x_data.append(i+1.5)
            y_data.append(cwds[i])

    # plot the line
    plt.plot(
        x_data,
        y_data,
        'r--'
    )
    # plot the markers
    plt.plot(
        x_data,
        y_data,
        'bx'
    )

    plt.xlabel('Corpus versions')
    plt.ylabel('Confidence-weighted delta')

    ax.set_xticks(x_ticks)
    ax.set_xticklabels(corpora)

    for lim in [0.25, 0.5, 0.75, 1.0]:
        if lim > max(y_data):
            break
    plt.ylim((0, lim))

    plt.savefig(
        outf,
        format='png',
        bbox_inches='tight',
        extra_artists=[]
    )

    plt.close()

def pairwiseSimilarityAnalysis(corpora, means, stds, outf=None, figsize=(5,5), font_size=18):
    font = {
        'family' : 'sans-serif',
        'size'   : font_size
    }
    matplotlib.rc('font', **font)

    (fig, ax) = plt.subplots(figsize=figsize)

    x_ticks = np.arange(len(corpora))
    x_data, y_data, y_err = [], [], []
    for i in range(len(corpora)):
        if not (means[i] is None):
            x_data.append(i)
            y_data.append(means[i])
            y_err.append(stds[i])

    # plot the markers
    plt.plot(
        x_data,
        y_data,
        'b.'
    )
    # plot the line
    plt.errorbar(
        x_data,
        y_data,
        yerr=y_err,
        fmt='r--'
    )

    plt.xlabel('Corpus versions', labelpad=15)
    plt.ylabel('Agg. Cos. Sim.', labelpad=10)

    ax.set_xticks(x_ticks)
    ax.set_xticklabels(corpora)
    for tick in ax.xaxis.get_majorticklabels():
        tick.set_horizontalalignment('left')

    plt.xticks(rotation=-60)

    #for lim in [0.25, 0.5, 0.75, 1.0]:
    #    if lim > max(y_data):
    #        break
    plt.ylim((0, 1))

    plt.savefig(
        outf,
        format='png',
        bbox_inches='tight',
        extra_artists=[]
    )

    plt.close()


def internalConfidenceAnalysis(corpora, confidences, threshold, outf=None, figsize=(5,5), font_size=18):
    font = {
        'family' : 'sans-serif',
        'size'   : font_size
    }
    matplotlib.rc('font', **font)

    (fig, ax) = plt.subplots(figsize=figsize)

    x_ticks = np.arange(len(corpora))
    x_data, y_data = [], []
    for i in range(len(corpora)):
        if not (confidences[i] is None):
            x_data.append(i)
            y_data.append(confidences[i])

    # plot the threshold
    plt.plot(
        np.arange(len(corpora)),
        [threshold for _ in range(len(corpora))],
        '--',
        color='#dddddd'
    )
    # plot the line
    plt.plot(
        x_data,
        y_data,
        'r--'
    )
    # plot the markers
    plt.plot(
        x_data,
        y_data,
        'b.'
    )

    plt.xlabel('Corpus versions', labelpad=15)
    plt.ylabel('Confidence', labelpad=10)

    ax.set_xticks(x_ticks)
    ax.set_xticklabels(corpora)
    for tick in ax.xaxis.get_majorticklabels():
        tick.set_horizontalalignment('left')

    plt.xticks(rotation=-60)
    plt.ylim((0, 1))

    plt.savefig(
        outf,
        format='png',
        bbox_inches='tight',
        extra_artists=[]
    )

    plt.close()
