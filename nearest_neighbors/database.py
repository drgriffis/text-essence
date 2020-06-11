import sqlite3
import os
from .data_models import *

class EmbeddingNeighborhoodDatabase:
    
    def __init__(self, fpath):
        if not os.path.exists(fpath):
            self._connection = sqlite3.connect(fpath)
            self._cursor = self._connection.cursor()
            self._build()
        else:
            self._connection = sqlite3.connect(fpath)
            self._cursor = self._connection.cursor()

    def _build(self):
        ## the EntityOverlapAnalysis table stores outputs from paired
        ## neighborhood analysis
        self._cursor.execute('''
        CREATE TABLE EntityOverlapAnalysis
        (
            Source text,
            Target text,
            AtK int,
            EntityKey text,
            SourceConfidence real,
            TargetConfidence real,
            ENSimilarity real,
            UNIQUE(Source, Target, AtK, EntityKey)
        )
        ''')

        ## flush all changes to DB
        self._connection.commit()

    def insertOrUpdate(self, objects):
        if (not type(objects) is list) and (not type(objects) is tuple):
            objects = [objects]

        if type(objects[0]) is EntityOverlapAnalysis:
            self.insertOrUpdateIntoEntityOverlapAnalysis(objects)

    def insertOrUpdateIntoEntityOverlapAnalysis(self, overlaps):
        if (not type(overlaps) is list) and (not type(overlaps) is tuple):
            overlaps = [overlaps]
            
        rows = [
            (
                o.source, o.target, o.at_k, o.key, o.source_confidence,
                o.target_confidence, o.EN_similarity
            )
                for o in overlaps
        ]

        self._cursor.executemany(
            '''
            REPLACE INTO EntityOverlapAnalysis VALUES (
                ?, ?, ?, ?, ?, ?, ?
            )
            ''',
            rows
        )

        self._connection.commit()
