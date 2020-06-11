import sqlite3
import os
from .data_models import *

class EmbeddingNeighborhoodDatabase:
    
    def __init__(self, fpath):
        self._connection = sqlite3.connect(fpath)
        self._cursor = self._connection.cursor()
        self._build()

    def _build(self):
        ## the EntityOverlapAnalysis table stores outputs from paired
        ## neighborhood analysis
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS EntityOverlapAnalysis
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


        ## the AggregateNearestNeighbors table stores nearest neighbors
        ## aggregated across multiple source runs
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS AggregateNearestNeighbors
        (
            Source text,
            Target text,
            EntityKey text,
            NeighborKey text,
            MeanDistance real,
            UNIQUE(Source, Target, EntityKey, NeighborKey)
        )
        ''')


        ## the EntityTerms table maps entity keys to string terms
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS EntityTerms
        (
            EntityKey text,
            Term text,
            Preferred int,
            UNIQUE(EntityKey, Term)
        )
        ''')

        ## flush all changes to DB
        self._connection.commit()

    def insertOrUpdate(self, objects):
        if (not type(objects) is list) and (not type(objects) is tuple):
            objects = [objects]

        if type(objects[0]) is EntityOverlapAnalysis:
            self.insertOrUpdateIntoEntityOverlapAnalysis(objects)
        elif type(objects[0]) is AggregateNearestNeighbor:
            self.insertOrUpdateIntoAggregateNearestNeighbors(objects)
        elif type(objects[0]) is EntityTerm:
            self.insertOrUpdateIntoEntityTerms(objects)

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

    def insertOrUpdateIntoAggregateNearestNeighbors(self, nbrs):
        if (not type(nbrs) is list) and (not type(nbrs) is tuple):
            nbrs = [nbrs]

        rows = [
            (
                n.source, n.target, n.key, n.neighbor_key, n.mean_distance
            )
                for n in nbrs
        ]

        self._cursor.executemany(
            '''
            REPLACE INTO AggregateNearestNeighbors VALUES (
                ?, ?, ?, ?, ?
            )
            ''',
            rows
        )

        self._connection.commit()

    def insertOrUpdateIntoEntityTerms(self, ent_terms):
        if (not type(ent_terms) is list) and (not type(ent_terms) is tuple):
            ent_terms = [ent_terms]

        rows = [
            (
                et.entity_key, et.term, et.preferred
            )
                for et in ent_terms
        ]

        self._cursor.executemany(
            '''
            REPLACE INTO EntityTerms VALUES (
                ?, ?, ?
            )
            ''',
            rows
        )

        self._connection.commit()
