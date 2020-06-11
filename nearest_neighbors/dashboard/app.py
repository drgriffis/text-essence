from flask import Flask
from flask import render_template
from flask import request
app = Flask(__name__)

import os
import configparser
from nearest_neighbors.database import EmbeddingNeighborhoodDatabase
from nearest_neighbors.dashboard import packaging

config = configparser.ConfigParser()
config.read('config.ini')

@app.route('/showchanges', methods=['POST'])
@app.route('/showchanges/<src>/<trg>/<at_k>', methods=['GET', 'POST'])
def showChanges(src=None, trg=None, at_k=None):
    if request.method == 'GET':
        getter = request.args.get
    else:
        getter = request.form.get

    if src is None:
        src = getter('src', None)
    if trg is None:
        trg = getter('trg', None)
    if at_k is None:
        at_k = getter('at_k', None)

    db = EmbeddingNeighborhoodDatabase(config['PairedNeighborhoodAnalysis']['DatabaseFile'])

    top_cwd, top_cws = [], []
    cwd_rows = db.selectFromEntityOverlapAnalysis(
        src,
        trg,
        at_k,
        source_confidence_threshold=0.5,
        target_confidence_threshold=0.5,
        order_by='CWD DESC',
        limit=10
    )
    for row in cwd_rows:
        top_cwd.append({
            'Key': row.key,
            'String': row.string,
            'SourceConfidence': packaging.prettify(row.source_confidence, decimals=2),
            'TargetConfidence': packaging.prettify(row.target_confidence, decimals=2),
            'ENSimilarity': packaging.prettify(row.EN_similarity, decimals=2),
            'CWD': packaging.prettify(row.CWD, decimals=2)
        })

    cws_rows = db.selectFromEntityOverlapAnalysis(
        src,
        trg,
        at_k,
        source_confidence_threshold=0.5,
        target_confidence_threshold=0.5,
        order_by='CWS DESC',
        limit=10
    )
    for row in cws_rows:
        top_cws.append({
            'Key': row.key,
            'String': row.string,
            'SourceConfidence': packaging.prettify(row.source_confidence, decimals=2),
            'TargetConfidence': packaging.prettify(row.target_confidence, decimals=2),
            'ENSimilarity': packaging.prettify(row.EN_similarity, decimals=2),
            'CWS': packaging.prettify(row.CWS, decimals=2)
        })

    db.close()

    return render_template(
        'showchanges.html',
        top_cwd=top_cwd,
        top_cws=top_cws,
        src=src,
        trg=trg
    )


@app.route('/neighbors', methods=['POST'])
@app.route('/neighbors/<query_key>', methods=['GET', 'POST'])
def neighbors(query_key=None):
    if request.method == 'GET':
        getter = request.args.get
    else:
        getter = request.form.get

    if query_key is None:
        query_key = getter('query_key', None)
    
    corpora = getter('corpora', '').split(',')

    db = EmbeddingNeighborhoodDatabase(config['PairedNeighborhoodAnalysis']['DatabaseFile'])

    tables = []
    for corpus in corpora:
        rows = db.selectFromAggregateNearestNeighbors(
            corpus,
            corpus,
            query_key,
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
        })

    return render_template(
        'neighbors.html',
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
