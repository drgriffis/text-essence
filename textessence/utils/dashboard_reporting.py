import os
import configparser
from collections import OrderedDict
from hedgepig_logger import log
from textessence.lib import normalization
from textessence.corpus.cord19.extract_cord19_articles import CORD19ExtractionReport
from textessence.corpus.snapshot_data_models import *
from textessence.corpus.compile_snapshot_corpus import CompilationData
from textessence.terminology.terminology_data_models import *

class SourceCorporaReport:
    def __init__(self, sources_config):
        self.corpus_statuses = OrderedDict()
        for corpus in sources_config['Default']['CorporaInUse'].split(','):
            if corpus in sources_config:
                corpus_config = sources_config[corpus]
                extraction_report = CORD19ExtractionReport(corpus_config['RootDirectory'])
                self.corpus_statuses[corpus] = SourceCorpusStatus(
                    corpus,
                    extraction_report
                )
            else:
                log.writeln('[WARNING] CorporaInUse setting includes corpus "{0}",'
                            ' which was not found in the config file'.format(corpus))

    def renderToLog(self):
        log.writeln('=======================================================')
        log.writeln('==              Source Corpora Report                ==')
        log.writeln('=======================================================')
        for (corpus, status) in self.corpus_statuses.items():
            log.writeln('{0} --> {1} {2}'.format(
                corpus,
                'X' if status.extracted else ' ',
                '' if (not status.extracted) else '(Extracted on: {0})'.format(
                    status.extraction_report.extraction_timestamp
                        .strftime(CORD19ExtractionReport.TIMESTAMP_FORMAT)
                )
            ))

class SourceCorpusStatus:
    def __init__(self, corpus, extraction_report):
        self.corpus = corpus
        self.extracted = not (extraction_report.extraction_timestamp is None)
        self.extraction_report = extraction_report


class TerminologiesReport:
    def __init__(self, base_config, terminologies_config):
        normalization_options = normalization.loadConfiguration(base_config['Normalization'])

        terminologies_root_dir = terminologies_config['Default']['RootDirectory']
        self.collection = TerminologyCollection(
            terminologies_root_dir
        )

        self.terminology_statuses = OrderedDict()
        for terminology in terminologies_config['Default']['TerminologiesInUse'].split(','):
            if terminology in terminologies_config:
                self.terminology_statuses[terminology] = TerminologyStatus(
                    self.collection,
                    terminology,
                    terminologies_config,
                    normalization_options
                )
                terminology_config = terminologies_config[terminology]
            else:
                log.writeln('[WARNING] TerminologiesInUse setting includes terminology "{0}",'
                            ' which was not found in the config file'.format(terminology))

    def renderToLog(self):
        log.writeln('=======================================================')
        log.writeln('==               Terminologies Report                ==')
        log.writeln('=======================================================')

        max_terminology_len = max([len(t) for t in self.terminology_statuses.keys()])
        fmt = lambda term, extr, cmap, filt, prep, comp: (
            '{0:<%d} | {1:<9} | {2:<9} | {3:<8} | {4:<12} | {5:<8}' % (max_terminology_len+1)
        ).format(term, extr, cmap, filt, prep, comp)

        log.writeln(fmt(
            '', 'Extracted', 'CatMapped', 'Filtered', 'Preprocessed', 'Compiled'
        ))
        for (terminology, status) in self.terminology_statuses.items():
            log.writeln(fmt(
                terminology,
                'X' if status.extracted else '',
                'X' if status.categories_mapped else '',
                'X' if status.filtered else '',
                'X' if status.preprocessed else '',
                'X' if status.compiled else ''
            ))


class TerminologyStatus:
    def __init__(self, collection, terminology, terminologies_config, normalization_options):
        self.terminology = terminology
        self.terminology_config = terminologies_config[self.terminology]

        terminology_data = collection[self.terminology]

        self.extracted = (
            os.path.exists(terminology_data.raw_terminology_file)
        )

        self.categories_mapped = (
            os.path.exists(terminology_data.category_map_file)
        )

        self.filtered = (
            os.path.exists(terminology_data.filtered_terminology_file)
        )

        self.preprocessed = (
            os.path.exists(
                terminology_data.preprocessed_terminology_file(normalization_options)
            )
        )

        self.compiled = (
            os.path.exists(
                terminology_data.compiled_preprocessed_terminology_file(normalization_options)
            )
        )


class SnapshotCorporaReport:
    def __init__(self, base_config, snapshots_config):
        normalization_options = normalization.loadConfiguration(base_config['Normalization'])

        snapshots_root_dir = snapshots_config['Default']['SnapshotsRootDirectory']
        self.collection = LiteratureSnapshotCollection(
            snapshots_root_dir
        )

        self.snapshot_statuses = OrderedDict()
        for snapshot in snapshots_config['Default']['SnapshotsInUse'].split(','):
            if snapshot in snapshots_config:
                self.snapshot_statuses[snapshot] = SnapshotCorpusStatus(
                    self.collection,
                    snapshot,
                    snapshots_config,
                    normalization_options
                )
            else:
                log.writeln('[WARNING] SnapshotsInUse setting includes snapshot "{0}",'
                            ' which was not found in the config file'.format(snapshot))

    def renderToLog(self):
        log.writeln('=======================================================')
        log.writeln('==             Snapshot Corpora Report               ==')
        log.writeln('=======================================================')

        max_snapshot_len = max([len(s) for s in self.snapshot_statuses.keys()])
        fmt = lambda snap, comp, prep, recomp: (
            '{0:<%d} | {1:<8} | {2:<12} | {3:<9}' % (max_snapshot_len+1)
        ).format(snap, comp, prep, recomp)

        log.writeln(fmt(
            'Snapshot', 'Compiled', 'Preprocessed', 'Recompile?'
        ))
        for (snapshot, status) in self.snapshot_statuses.items():
            log.writeln(fmt(
                snapshot,
                'X' if status.compiled else '',
                'X' if status.preprocessed else '',
                'RECOMPILE' if status.recompile else ''
            ))


class SnapshotCorpusStatus:
    def __init__(self, collection, snapshot, snapshots_config, normalization_options):
        self.snapshot = snapshot
        self.snapshot_config = snapshots_config[self.snapshot]
        compilation_data = CompilationData(
            collection,
            self.snapshot,
            self.snapshot_config
        )

        # if this snapshot has been compiled, check to see if it's changed
        # since most recent compilation
        if compilation_data.previous_version_exists:
            if compilation_data.changed_since_last_compile:
                self.compiled = False
                self.recompile = True
            else:
                self.compiled = True
                self.recompile = False
        else:
            self.compiled = False
            self.recompile = False

        if self.compiled and (not self.recompile):
            self.preprocessed = os.path.exists(
                compilation_data.corpus.preprocessed_corpus_file(normalization_options)
            )
        else:
            self.preprocessed = False


class EmbeddingAnalysisReport:
    def __init__(self):
        pass


class DashboardMasterReport:
    def __init__(self, base_config, cord19_config, snapshot_config, terminology_config):
        self.source_corpora_report = SourceCorporaReport(cord19_config)
        self.snapshot_corpora_report = SnapshotCorporaReport(base_config, snapshot_config)
        self.terminologies_report = TerminologiesReport(base_config, terminology_config)

    def renderToLog(self):
        self.source_corpora_report.renderToLog()
        log.writeln('\n\n')
        self.snapshot_corpora_report.renderToLog()
        log.writeln('\n\n')
        self.terminologies_report.renderToLog()



if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-c', '--config', dest='config_f',
            help='(required) configuration file')
        (options, args) = parser.parse_args()
        if not options.config_f:
            parser.print_help()
            parser.error('Must provide --config')
        return options
    options = _cli()

    base_config = configparser.ConfigParser()
    base_config.read(options.config_f)

    cord19_config = configparser.ConfigParser()
    cord19_config.read(base_config['General']['CORD19Config'])

    snapshot_config = configparser.ConfigParser()
    snapshot_config.read(base_config['General']['SnapshotConfig'])

    terminology_config = configparser.ConfigParser()
    terminology_config.read(base_config['General']['TerminologyConfig'])

    logfile = base_config['Logging']['OverviewReport']
    log.start(logfile)
    log.writeConfig([
        ('Base configuration file', options.config_f),
        ('Snapshot configuration file', base_config['General']['SnapshotConfig']),
        ('Terminology configuration file', base_config['General']['TerminologyConfig']),
    ], 'Overview Dashboard Report')

    report = DashboardMasterReport(base_config, cord19_config, snapshot_config, terminology_config)
    report.renderToLog()

    log.stop()
