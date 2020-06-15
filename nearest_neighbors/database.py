import sqlite3
import os
import math
from .data_models import *

class EmbeddingType:
    ENTITY = 0
    TERM = 1
    WORD = 2
    CONTEXT = 3

    @staticmethod
    def parse(string):
        if string.strip().upper() == 'ENTITY':
            return EmbeddingType.ENTITY
        elif string.strip().upper() == 'TERM':
            return EmbeddingType.TERM
        elif string.strip().upper() == 'WORD':
            return EmbeddingType.WORD
        elif string.strip().upper() == 'CONTEXT':
            return EmbeddingType.CONTEXT
        else:
            raise ValueError('EmbeddingType "%s" not known' % string)

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
        ## (NB nearest neighbors are calculated within Source only; the
        ##  AggregateNearestNeighborSubsets table manages identifying the
        ##  subset of neighbors included in a given Source/Target pair)
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS AggregateNearestNeighbors
        (
            ID INTEGER PRIMARY KEY,
            Source text,
            EntityKey text,
            NeighborKey text,
            NeighborType int,
            MeanDistance real,
            UNIQUE(Source, EntityKey, NeighborKey)
        )
        ''')


        ## the AggregateNearestNeighborSubsets table indexes which nearest
        ## neighbors are included in which source/target pairs
        ## (NB nearest neighbors are calculated within Source only, so the
        ##  actual neighbors can be stored for reuse in
        ##  AggregateNearestNeighbors)
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS AggregateNearestNeighborSubsets
        (
            Source text,
            Target text,
            NeighborID int,
            UNIQUE(Source, Target, NeighborID),
            CONSTRAINT FK_NeighborID
                FOREIGN KEY (NeighborID)
                REFERENCES AggregateNearestNeighbors(ID)
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

    def insertOrUpdate(self, objects, *args, **kwargs):
        if (not type(objects) is list) and (not type(objects) is tuple):
            objects = [objects]

        if type(objects[0]) is EntityOverlapAnalysis:
            self.insertOrUpdateIntoEntityOverlapAnalysis(objects, *args, **kwargs)
        elif type(objects[0]) is AggregateNearestNeighbor:
            self.insertOrUpdateIntoAggregateNearestNeighbors(objects, *args, **kwargs)
        elif type(objects[0]) is EntityTerm:
            self.insertOrUpdateIntoEntityTerms(objects, *args, **kwargs)

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

    def insertOrUpdateIntoAggregateNearestNeighbors(self, nbrs, neighbor_type=EmbeddingType.ENTITY):
        if (not type(nbrs) is list) and (not type(nbrs) is tuple):
            nbrs = [nbrs]
        
        ## since each neighbor relationship may or may not need to be added to
        ## the AggregateNearestNeighbors table as well as to
        ## AggregateNearestNeighborSubsets, process rows one by one
        for nbr in nbrs:
            
            ## (1) check if it's already in AggregateNearestNeighbors
            query = '''
            SELECT ID, MeanDistance FROM AggregateNearestNeighbors
            WHERE
                Source=?
                AND EntityKey=?
                AND NeighborKey=?
                AND NeighborType=?
            '''
            args = [nbr.source, nbr.key, nbr.neighbor_key, neighbor_type]
            self._cursor.execute(query, args)

            nbr_info = self._cursor.fetchone()

            ## (2.1) if it isn't, add it to AggregateNearestNeighbors
            if nbr_info is None:
                row = (nbr.source, nbr.key, nbr.neighbor_key, neighbor_type, nbr.mean_distance)
                self._cursor.execute(
                    '''
                    INSERT INTO
                        AggregateNearestNeighbors
                        (
                            Source, EntityKey, NeighborKey, NeighborType, MeanDistance
                        )
                    VALUES (
                        ?, ?, ?, ?, ?
                    )
                    ''',
                    row
                )

                # pull the ID of the new row
                nbr_ID = self._cursor.lastrowid

            ## (2.2) if it is, just make sure the distance is the same, as a sanity check
            else:
                (nbr_ID, mean_dist) = nbr_info
                # fuzzy equality check to account for floating point errors
                if not math.isclose(mean_dist, nbr.mean_distance, abs_tol=0.001):
                    print('[WARNING] Conflict in record for {0} <-> {1} in {2}'.format(nbr.key, nbr.neighbor_key, nbr.source))
                    print('  Saved distance: {0}'.format(mean_dist))
                    print('  Distance provided: {0}'.format(nbr.mean_distance))
                    yn, acceptable = '', set(['y', 'n'])
                    while not yn.strip().lower() in acceptable:
                        yn = input('Proceed? [y/n] ')
                    if yn.strip().lower() == 'n':
                        print('Rolling back and aborting.')
                        self._connection.rollback()
                        exit(1)

            ## (3) finally, add the source/target relationship to
            ##     AggregateNearestNeighborSubsets
            row = (nbr.source, nbr.target, nbr_ID)
            self._cursor.execute(
                '''
                REPLACE INTO AggregateNearestNeighborSubsets VALUES (
                    ?, ?, ?
                )
                ''',
                row
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
            ann.Source,
            anns.Target,
            ann.EntityKey,
            ann.NeighborKey,
            ann.MeanDistance,
            et_query.Term as QueryTerm,
            et_nbr.Term as NeighborTerm
        FROM
            AggregateNearestNeighbors AS ann
            INNER JOIN
                AggregateNearestNeighborSubsets AS anns
                ON
                    anns.NeighborID = ann.ID
            INNER JOIN
                EntityTerms AS et_query
                ON
                    et_query.EntityKey = ann.EntityKey
                    AND et_query.Preferred = 1
            INNER JOIN
                EntityTerms AS et_nbr
                ON
                    et_nbr.EntityKey = ann.NeighborKey
                    AND et_nbr.Preferred = 1
        WHERE
            ann.Source=?
            AND anns.Target=?
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


    def searchInEntityTerms(self, query_string):
        query = '''
        SELECT
            *
        FROM
            EntityTerms
        WHERE
            EntityKey LIKE ?
            OR Term LIKE ?
        '''

        args = [query_string, query_string]

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
