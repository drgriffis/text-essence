from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify
app = Flask(__name__)

import os
import configparser
from nearest_neighbors.database import *
from nearest_neighbors.dashboard import packaging
from nearest_neighbors.dashboard import visualization

config = configparser.ConfigParser()
config.read('config.ini')

@app.route('/showchanges', methods=['POST'])
@app.route('/showchanges/<src>/<trg>/<filter_set>/<at_k>', methods=['GET', 'POST'])
def showChanges(src=None, trg=None, filter_set=None, at_k=None):
    if request.method == 'GET':
        getter = request.args.get
    else:
        getter = request.form.get

    if src is None:
        src = getter('src', None)
    if trg is None:
        trg = getter('trg', None)
    if filter_set is None:
        filter_set = getter('filter_set', None)
    if at_k is None:
        at_k = getter('at_k', None)

    db = EmbeddingNeighborhoodDatabase(config['PairedNeighborhoodAnalysis']['DatabaseFile'])

    top_cwd, bottom_cwd = [], []
    rows = db.selectFromEntityOverlapAnalysis(
        src,
        trg,
        filter_set,
        at_k,
        source_confidence_threshold=0.5,
        target_confidence_threshold=0.5,
        order_by='ConfidenceWeightedDelta DESC',
        limit=50
    )
    for row in rows:
        top_cwd.append({
            'Key': row.key,
            'String': row.string,
            'SourceConfidence': packaging.prettify(row.source_confidence, decimals=2),
            'TargetConfidence': packaging.prettify(row.target_confidence, decimals=2),
            'ENSimilarity': packaging.prettify(row.EN_similarity, decimals=2),
            'CWD': packaging.prettify(row.CWD, decimals=2)
        })

    rows = db.selectFromEntityOverlapAnalysis(
        src,
        trg,
        filter_set,
        at_k,
        source_confidence_threshold=0.5,
        target_confidence_threshold=0.5,
        order_by='ConfidenceWeightedDelta',
        limit=10
    )
    for row in rows:
        bottom_cwd.append({
            'Key': row.key,
            'String': row.string,
            'SourceConfidence': packaging.prettify(row.source_confidence, decimals=2),
            'TargetConfidence': packaging.prettify(row.target_confidence, decimals=2),
            'ENSimilarity': packaging.prettify(row.EN_similarity, decimals=2),
            'CWD': packaging.prettify(row.CWD, decimals=2)
        })

    db.close()

    return render_template(
        'showchanges.html',
        top_cwd=top_cwd,
        bottom_cwd=bottom_cwd,
        src=src,
        trg=trg
    )


@app.route('/info', methods=['POST'])
@app.route('/info/<query_key>', methods=['GET', 'POST'])
def info(query_key=None):
    if request.method == 'GET':
        getter = request.args.get
        lgetter = lambda key,default: request.args.get(key, default).split(',')
    else:
        getter = request.form.get
        lgetter = lambda key, default: request.form.getlist(key)

    if query_key is None:
        query_key = getter('query_key', None)
    
    corpora = lgetter('corpora', '')
    neighbor_type = getter('neighbor_type', 'ENTITY')
    neighbor_type = EmbeddingType.parse(neighbor_type)

    db = EmbeddingNeighborhoodDatabase(config['PairedNeighborhoodAnalysis']['DatabaseFile'])

    ## (1) get the nearest neighbors
    tables = []
    TABLES_PER_ROW = 3
    for i in range(len(corpora)):
        corpus = corpora[i]
        filter_set='.HC_{0}'.format(corpus)
        rows = db.selectFromAggregateNearestNeighbors(
            corpus,
            corpus,
            filter_set,
            query_key,
            neighbor_type=neighbor_type,
            limit=10
        )

        table_rows = []
        for row in rows:
            table_rows.append({
                'NeighborKey': row.neighbor_key,
                'NeighborString': row.neighbor_string,
                'Distance': row.mean_distance
            })

        tables.append({
            'Corpus': corpus,
            'Rows': table_rows,
            'IsGridRowStart': (i % TABLES_PER_ROW) == 0,
            'IsGridRowEnd': (i % TABLES_PER_ROW) == (TABLES_PER_ROW - 1),
            'IsCorpusTable': True
        })

    # add a placeholder for the "add a table" button
    tables.append({
        'IsCorpusTable': False
    })

    ## (2) get the terms for the entity
    all_terms = db.selectFromEntityTerms(
        query_key
    )
    term_list, preferred_term = [], ''
    for term in all_terms:
        if term.preferred == 1:
            preferred_term = term.term
        else:
            term_list.append(term.term)

    ## (3) get its change history
    corpora = [
        '2020-04-24',
        '2020-05-31',
        '2020-06-30',
        '2020-07-31',
        '2020-08-29',
        '2020-09-28',
        '2020-10-31'
    ]
    cwds = []
    for i in range(len(corpora)-1):
        change_src = corpora[i]
        change_trg = corpora[i+1]
        filter_set = '.HC_Union_{0}_{1}'.format(change_src, change_trg)  ## TODO HARD CODED
        at_k = 5  ## TODO HARD CODED

        rows = db.selectFromEntityOverlapAnalysis(
            change_src,
            change_trg,
            filter_set=filter_set,
            at_k=at_k,
            entity_key=query_key
        )
        rows = list(rows)
        if len(rows) == 1:
            cwds.append(rows[0].CWD)
        else:
            cwds.append(None)

    return render_template(
        'info.html',
        query_key=query_key,
        preferred_term=preferred_term,
        all_terms=sorted(term_list),
        corpora=(','.join(corpora)),
        tables=tables
    )


@app.route('/terms', methods=['POST'])
@app.route('/terms/<query_key>', methods=['GET', 'POST'])
def terms(query_key=None):
    if request.method == 'GET':
        getter = request.args.get
    else:
        getter = request.form.get

    if query_key is None:
        query_key = getter('query_key', None)

    db = EmbeddingNeighborhoodDatabase(config['PairedNeighborhoodAnalysis']['DatabaseFile'])

    rows = db.selectFromEntityTerms(
        query_key
    )

    table_rows = []
    for row in rows:
        table_rows.append({
            'Term': row.term,
            'Preferred': ('X' if row.preferred == 1 else '')
        })

    return render_template(
        'terms.html',
        rows=table_rows
    )


@app.route('/search', methods=['POST'])
@app.route('/search/<query>', methods=['GET', 'POST'])
def search(query=None):
    if request.method == 'GET':
        getter = request.args.get
    else:
        getter = request.form.get

    if query is None:
        query = getter('query', None)

    db = EmbeddingNeighborhoodDatabase(config['PairedNeighborhoodAnalysis']['DatabaseFile'])

    rows = db.searchInEntityTerms(
        query
    )

    table_rows = []
    for row in rows:
        table_rows.append({
            'Key': row.entity_key,
            'Term': row.term,
        })

    return render_template(
        'search.html',
        rows=table_rows,
        corpora='2020-03-27,2020-04-03'  ## hard-coded value for now
    )


@app.route('/_get_aggregate_nearest_neighbors_membership')
def getAggregateNearestNeighborsMembership():
    query_key = request.args.get('query_key', None)
    current_corpora = request.args.get('current_corpora', '')

    current_corpora = set(current_corpora.split(','))

    db = EmbeddingNeighborhoodDatabase(config['PairedNeighborhoodAnalysis']['DatabaseFile'])

    rows = db.findAggregateNearestNeighborsMembership(query_key)

    table_rows = []
    for row in sorted(rows):
        table_rows.append({
            'Source': row,
            'Checked': 1 if row in current_corpora else 0
        })

    return jsonify(table_rows)
