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
        top_cws=top_cws
    )
