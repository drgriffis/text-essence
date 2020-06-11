import sqlite3
import os
from .data_models import *

class EmbeddingNeighborhoodDatabase:
    
    def __init__(self, fpath):
        self._connection = sqlite3.connect(fpath)
        self._cursor = self._connection.cursor()
        self._build()

    def close(self):
        self._connection.close()

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


    def selectFromEntityOverlapAnalysis(self, src, trg, at_k,
            source_confidence_threshold=None, target_confidence_threshold=None,
            order_by='CWD', limit=10):

        base_query = '''
        SELECT
            eoa.*,
            (eoa.SourceConfidence * eoa.TargetConfidence * (eoa.SourceConfidence - eoa.ENSimilarity)) AS CWD,
            (eoa.SourceConfidence * eoa.TargetConfidence * eoa.ENSimilarity) AS CWS,
            et.Term
        FROM
            EntityOverlapAnalysis AS eoa
        INNER JOIN
            EntityTerms AS et
        ON
            et.EntityKey = eoa.EntityKey
        WHERE
            eoa.Source=?
            AND eoa.Target=?
            AND eoa.AtK=?
            AND et.Preferred=1
            {0}
        ORDER BY {1}
        LIMIT {2}
        '''

        args = [
            src,
            trg,
            at_k
        ]

        if not (source_confidence_threshold is None):
            src_conf_cond = 'AND eoa.SourceConfidence >= ?'
            args.append(source_confidence_threshold)
        else:
            src_conf_cond = ''

        if not (target_confidence_threshold is None):
            trg_conf_cond = 'AND eoa.TargetConfidence >= ?'
            args.append(target_confidence_threshold)
        else:
            trg_conf_cond = ''

        query = base_query.format(
            '{0} {1}'.format(src_conf_cond, trg_conf_cond),
            order_by,
            limit
        )

        self._cursor.execute(query, args)
        for row in self._cursor:
            (
                source,
                target,
                at_k,
                key,
                source_confidence,
                target_confidence,
                EN_similarity,
                CWD,
                CWS,
                preferred_term
            ) = row
            ret_obj = EntityOverlapAnalysis(
                source=source,
                target=target,
                at_k=at_k,
                key=key,
                source_confidence=source_confidence,
                target_confidence=target_confidence,
                EN_similarity=EN_similarity,
                CWD=CWD,
                CWS=CWS,
                string=preferred_term
            )
            yield ret_obj


    def selectFromAggregateNearestNeighbors(self, src, trg, key,
            limit=10):

        query = '''
        SELECT
            ann.*,
            et_query.Term as QueryTerm,
            et_nbr.Term as NeighborTerm
        FROM
            AggregateNearestNeighbors AS ann
        LEFT OUTER JOIN
            EntityTerms AS et_query
        ON
            et_query.EntityKey = ann.EntityKey
            AND et_query.Preferred = 1
        LEFT OUTER JOIN
            EntityTerms AS et_nbr
        ON
            et_nbr.EntityKey = ann.NeighborKey
            AND et_nbr.Preferred = 1
        WHERE
            ann.Source=?
            AND ann.Target=?
            AND ann.EntityKey=?
        ORDER BY ann.MeanDistance ASC
        LIMIT {0}
        '''.format(limit)

        args = [
            src,
            trg,
            key
        ]

        self._cursor.execute(query, args)
        for row in self._cursor:
            (
                source,
                target,
                entity_key,
                neighbor_key,
                mean_distance,
                query_term,
                neighbor_term
            ) = row
            ret_obj = AggregateNearestNeighbor(
                source=source,
                target=target,
                key=entity_key,
                string=query_term,
                neighbor_key=neighbor_key,
                neighbor_string=neighbor_term,
                mean_distance=mean_distance
            )
            yield ret_obj


    def selectFromEntityTerms(self, key):
        query = '''
        SELECT
            *
        FROM
            EntityTerms
        WHERE
            EntityKey=?
        '''

        args = [key]

        self._cursor.execute(query, args)
        for row in self._cursor:
            (
                entity_key,
                term,
                preferred
            ) = row
            ret_obj = EntityTerm(
                entity_key=entity_key,
                term=term,
                preferred=preferred
            )
            yield ret_obj
