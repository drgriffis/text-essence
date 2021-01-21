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
        ## the EmbeddingSetGroups table stores information about groups of
        ## embedding sets for analysis (i.e., a group of corpora or other
        ## embedding sources to be compared together)
        ## for simplicity, enforce that all groups must have unique titles
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS EmbeddingSetGroups
        (
            ID INTEGER PRIMARY KEY NOT NULL,
            ShortName text NOT NULL,
            DisplayTitle text,
            UNIQUE(ShortName)
        )
        ''')

        ## the EmbeddingSets table stores information about individual sets of
        ## embedding replicates for analysis
        ## for simplicity, require that no two embedding sets within the same
        ## group can have the same title
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS EmbeddingSets
        (
            ID INTEGER PRIMARY KEY NOT NULL,
            GroupID int NOT NULL,
            Name text NOT NULL,
            Ordering int NOT NULL,
            UNIQUE(GroupID, Name),
            UNIQUE(GroupID, Ordering),
            CONSTRAINT FK_GroupID
                FOREIGN KEY (GroupID)
                REFERENCES EmbeddingSetGroups(ID)
        )
        ''')

        ## the EntityOverlapAnalysis table stores outputs from paired
        ## neighborhood analysis
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS EntityOverlapAnalysis
        (
            Source int,
            Target int,
            FilterSet text,
            AtK int,
            EntityKey text,
            ENSimilarity real,
            UNIQUE(Source, Target, FilterSet, AtK, EntityKey),
            CONSTRAINT FK_Source
                FOREIGN KEY (Source)
                REFERENCES EmbeddingSets(ID),
            CONSTRAINT FK_Target
                FOREIGN KEY (Target)
                REFERENCES EmbeddingSets(ID)
        )
        ''')


        ## the InternalConfidence table stores outputs from self-paired
        ## neighborhood analysis
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS InternalConfidence
        (
            Source int,
            AtK int,
            EntityKey text,
            Confidence real,
            UNIQUE(Source, AtK, EntityKey),
            CONSTRAINT FK_Source
                FOREIGN KEY (Source)
                REFERENCES EmbeddingSets(ID)
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
            Source int,
            EntityKey text,
            NeighborKey text,
            NeighborType int,
            MeanDistance real,
            UNIQUE(Source, EntityKey, NeighborKey),
            CONSTRAINT FK_Source
                FOREIGN KEY (Source)
                REFERENCES EmbeddingSets(ID)
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
            Source int,
            Target int,
            FilterSet text,
            NeighborID int,
            UNIQUE(Source, Target, FilterSet, NeighborID),
            CONSTRAINT FK_NeighborID
                FOREIGN KEY (NeighborID)
                REFERENCES AggregateNearestNeighbors(ID),
            CONSTRAINT FK_Source
                FOREIGN KEY (Source)
                REFERENCES EmbeddingSets(ID),
            CONSTRAINT FK_Target
                FOREIGN KEY (Target)
                REFERENCES EmbeddingSets(ID)
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


        ## the EntityDefinitions table maps entity keys to string definitions
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS EntityDefinitions
        (
            EntityKey text,
            Definition text,
            UNIQUE(EntityKey, Definition)
        )
        ''')


        ## the AggregatePairwiseSimilarity table stores cosine similarity
        ## values between entity pairs within a given source corpus
        ## (calculated as the mean similarity over replicates)
        self._cursor.execute('''
        CREATE TABLE IF NOT EXISTS AggregatePairwiseSimilarity
        (
            Source int,
            EntityKey text,
            NeighborKey text,
            MeanSimilarity real,
            StdDevSimilarity real,
            UNIQUE(Source, EntityKey, NeighborKey),
            CONSTRAINT FK_Source
                FOREIGN KEY (Source)
                REFERENCES EmbeddingSets(ID)
        )
        ''')

        ## flush all changes to DB
        self._connection.commit()

    def insertOrUpdate(self, objects, *args, **kwargs):
        if (not type(objects) is list) and (not type(objects) is tuple):
            objects = [objects]

        if type(objects[0]) is EmbeddingSetGroup:
            self.insertOrUpdateIntoEmbeddingSetGroups(objects, *args, **kwargs)
        elif type(objects[0]) is EmbeddingSet:
            self.insertOrUpdateIntoEmbeddingSets(objects, *args, **kwargs)
        elif type(objects[0]) is EntityOverlapAnalysis:
            self.insertOrUpdateIntoEntityOverlapAnalysis(objects, *args, **kwargs)
        elif type(objects[0]) is InternalConfidence:
            self.insertOrUpdateIntoInternalConfidence(objects, *args, **kwargs)
        elif type(objects[0]) is AggregateNearestNeighbor:
            self.insertOrUpdateIntoAggregateNearestNeighbors(objects, *args, **kwargs)
        elif type(objects[0]) is EntityTerm:
            self.insertOrUpdateIntoEntityTerms(objects, *args, **kwargs)
        elif type(objects[0]) is EntityDefinition:
            self.insertOrUpdateIntoEntityDefinitions(objects, *args, **kwargs)
        elif type(objects[0]) is AggregatePairwiseSimilarity:
            self.insertOrUpdateIntoAggregatePairwiseSimilarity(objects, *args, **kwargs)

    def insertOrUpdateIntoEmbeddingSetGroups(self, groups):
        if (not type(groups) is list) and (not type(groups) is tuple):
            groups = [groups]

        new_rows, existing_rows = [], []
        for group in groups:
            if group.ID is None:
                new_rows.append(
                    (
                        (
                            group.short_name,
                            group.display_title
                        ), # row to add
                        group                # pointer back to the group to set its ID
                    )
                )
            else:
                existing_rows.append(
                    (group.ID, group.short_name, group.display_title)
                )

        if len(new_rows) > 0:
            for (row, group) in new_rows:
                self._cursor.execute(
                    '''
                    INSERT INTO
                        EmbeddingSetGroups
                        (
                            ShortName,
                            DisplayTitle
                        )
                    VALUES (
                        ?, ?
                    )
                    ''',
                    row
                )

                # set the ID of the group to the ID of the new row
                group.ID = self._cursor.lastrowid

        if len(existing_rows) > 0:
            self._cursor.executemany(
                '''
                REPLACE INTO
                    EmbeddingSetGroups
                VALUES (
                    ?, ?, ?
                )
                ''',
                existing_rows
            )

        self._connection.commit()

    def insertOrUpdateIntoEmbeddingSets(self, embedding_sets):
        if (not type(embedding_sets) is list) and (not type(embedding_sets) is tuple):
            embedding_sets = [embedding_sets]

        # rely on uniqueness of shortname to make sure all embedding set
        # groups exist
        groups_by_short_name = {}
        for e_set in embedding_sets:
            groups_by_short_name[e_set.group.short_name] = e_set.group
        self.insertOrUpdateIntoEmbeddingSetGroups(list(groups_by_short_name.values()))

        new_rows, existing_rows = [], []
        for e_set in embedding_sets:
            if e_set.ID is None:
                new_rows.append(
                    (
                        # row to add
                        (e_set.group.ID, e_set.name, e_set.ordering),
                        # pointer back to the set to set its ID
                        e_set
                    )
                )
            else:
                existing_rows.append(
                    (e_set.ID, e_set.group.ID, e_set.name, e_set.ordering),
                )

        if len(new_rows) > 0:
            for (row, e_set) in new_rows:
                self._cursor.execute(
                    '''
                    INSERT INTO
                        EmbeddingSets
                        (
                            GroupID,
                            Name,
                            Ordering
                        )
                    VALUES (
                        ?, ?, ?
                    )
                    ''',
                    row
                )

                # set the ID of the group to the ID of the new row
                e_set.ID = self._cursor.lastrowid

        if len(existing_rows) > 0:
            self._cursor.executemany(
                '''
                REPLACE INTO
                    EmbeddingSets
                VALUES (
                    ?, ?, ?, ?
                )
                ''',
                existing_rows
            )

        self._connection.commit()

    def _saveLinkedEmbeddingSets(self, objects, getter):
        # rely on uniqueness of group title + set name to make sure all
        # embedding sets exist
        sets_by_identifier = {}
        for obj in objects:
            e_set = getter(obj)
            e_set_identifier = '{0}_{1}'.format(
                e_set.group.short_name,
                e_set.name
            )
            sets_by_identifier[e_set_identifier] = e_set
        self.insertOrUpdateIntoEmbeddingSets(list(sets_by_identifier.values()))
        

    def insertOrUpdateIntoEntityOverlapAnalysis(self, overlaps):
        if (not type(overlaps) is list) and (not type(overlaps) is tuple):
            overlaps = [overlaps]

        # first flush source and target embedding sets to the DB
        self._saveLinkedEmbeddingSets(overlaps, lambda o: o.source)
        self._saveLinkedEmbeddingSets(overlaps, lambda o: o.target)
            
        rows = [
            (
                o.source.ID,
                o.target.ID,
                o.filter_set,
                o.at_k,
                o.key,
                o.EN_similarity
            )
                for o in overlaps
        ]

        self._cursor.executemany(
            '''
            REPLACE INTO EntityOverlapAnalysis VALUES (
                ?, ?, ?, ?, ?, ?
            )
            ''',
            rows
        )

        self._connection.commit()

    def insertOrUpdateIntoInternalConfidence(self, confidences):
        if (not type(confidences) is list) and (not type(confidences) is tuple):
            confidences = [confidences]

        # first flush source embedding sets to the DB
        self._saveLinkedEmbeddingSets(confidences, lambda c: c.source)
            
        rows = [
            (
                c.source.ID,
                c.at_k,
                c.key,
                c.confidence
            )
                for c in confidences
        ]

        self._cursor.executemany(
            '''
            REPLACE INTO InternalConfidence VALUES (
                ?, ?, ?, ?
            )
            ''',
            rows
        )

        self._connection.commit()

    def insertOrUpdateIntoAggregateNearestNeighbors(self, nbrs, neighbor_type=EmbeddingType.ENTITY):
        if (not type(nbrs) is list) and (not type(nbrs) is tuple):
            nbrs = [nbrs]

        # first flush source and target embedding sets to the DB
        self._saveLinkedEmbeddingSets(nbrs, lambda n: n.source)
        self._saveLinkedEmbeddingSets(nbrs, lambda n: n.target)
        
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
            args = [
                nbr.source.ID,
                nbr.key,
                nbr.neighbor_key,
                neighbor_type
            ]
            self._cursor.execute(query, args)

            nbr_info = self._cursor.fetchone()

            ## (2.1) if it isn't, add it to AggregateNearestNeighbors
            if nbr_info is None:
                row = (
                    nbr.source.ID,
                    nbr.key,
                    nbr.neighbor_key,
                    neighbor_type,
                    nbr.mean_distance
                )
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
            row = (
                nbr.source.ID,
                nbr.target.ID,
                nbr.filter_set,
                nbr_ID
            )
            self._cursor.execute(
                '''
                REPLACE INTO AggregateNearestNeighborSubsets VALUES (
                    ?, ?, ?, ?
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
                et.entity_key,
                et.term,
                et.preferred
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

    def insertOrUpdateIntoEntityDefinitions(self, ent_defns):
        if (not type(ent_defns) is list) and (not type(ent_defns) is tuple):
            ent_defns = [ent_defns]

        rows = [
            (
                ed.entity_key,
                ed.definition
            )
                for ed in ent_defns
        ]

        self._cursor.executemany(
            '''
            REPLACE INTO EntityDefinitions VALUES (
                ?, ?
            )
            ''',
            rows
        )

        self._connection.commit()

    def insertOrUpdateIntoAggregatePairwiseSimilarity(self, sims):
        if (not type(sims) is list) and (not type(sims) is tuple):
            sims = [sims]

        # first make sure linked embedding sets are saved to the DB
        self._saveLinkedEmbeddingSets(sims, lambda s: s.source)

        rows = [
            (
                s.source.ID,
                s.key,
                s.neighbor_key,
                float(s.mean_similarity),
                float(s.std_similarity)
            )
                for s in sims
        ]

        self._cursor.executemany(
            '''
            REPLACE INTO AggregatePairwiseSimilarity VALUES (
                ?, ?, ?, ?, ?
            )
            ''',
            rows
        )

        self._connection.commit()


    def getOrCreateEmbeddingSetGroup(self, short_name):
        groups = list(self.selectFromEmbeddingSetGroups(short_name=short_name))
        if len(groups) == 0:
            group = EmbeddingSetGroup(
                short_name=short_name
            )
            self.insertOrUpdate(group)
        else:
            group = groups[0]
        return group

    def getOrCreateEmbeddingSet(self, name, group_ID=None, group_name=None):
        if group_ID is None and group_name is None:
            raise TypeError("getOrCreateEmbeddingSet() must be called with either group_ID or group_name keyword argument")

        if group_ID:
            group = list(self.selectFromEmbeddingSetGroups(ID=group_ID))[0]
        else:
            group = self.getOrCreateEmbeddingSetGroup(short_name=group_name)

        sets = list(self.selectFromEmbeddingSets(group_ID=group.ID, name=name))
        if len(sets) == 0:
            # count the number of existing embedding sets within this group
            other_sets = list(self.selectFromEmbeddingSets(group_ID=group.ID))
            # and append the new set at the end
            source_set = EmbeddingSet(
                group=group,
                name=name,
                ordering=len(other_sets)+1
            )
            self.insertOrUpdate(e_set)
        else:
            e_set = sets[0]

        return e_set


    def selectFromEmbeddingSetGroups(self, ids=None, short_name=None):
        if ids:
            try:
                _ = iter(ids)
                ids = list(ids)
            except TypeError:
                ids = [ids]

        base_query = '''
        SELECT
            *
        FROM
            EmbeddingSetGroups
        WHERE
            {0}
        '''

        conditions, args = [], []

        if ids:
            conditions.append('ID IN (?)')
            args.append(','.join([str(i) for i in ids]))
        if short_name:
            conditions.append('ShortName = ?')
            args.append(short_name)

        query = base_query.format(
            ' AND '.join(conditions)
        )

        self._cursor.execute(query, args)
        for row in self._cursor:
            (
                ID,
                short_name,
                display_title
            ) = row
            ret_obj = EmbeddingSetGroup(
                ID=ID,
                short_name=short_name,
                display_title=display_title
            )
            yield ret_obj


    def selectFromEmbeddingSets(self, ids=None, group_ID=None, name=None):
        if ids:
            try:
                _ = iter(ids)
                ids = list(ids)
            except TypeError:
                ids = [ids]

        base_query = '''
        SELECT
            *
        FROM
            EmbeddingSets
        WHERE
            {0}
        '''

        conditions, args = [], []

        if ids:
            conditions.append('ID IN (?)')
            args.append(','.join([str(i) for i in ids]))
        if group_ID:
            conditions.append('GroupID = ?')
            args.append(group_ID)
        if name:
            conditions.append('Name = ?')
            args.append(name)

        query = base_query.format(
            ' AND '.join(conditions)
        )

        self._cursor.execute(query, args)
        raw_rows = list(self._cursor)

        # fetch the associated group objects
        group_IDs = set([
            row[1] for row in raw_rows
        ])
        groups = self.selectFromEmbeddingSetGroups(ids=group_IDs)
        groups_by_ID = { group.ID: group for group in groups }

        # instantiate the embedding set objects
        for row in raw_rows:
            (
                ID,
                groupID,
                name,
                ordering
            ) = row
            ret_obj = EmbeddingSet(
                ID=ID,
                group=groups_by_ID[groupID],
                name=name,
                ordering=ordering
            )
            yield ret_obj


    def selectFromEntityOverlapAnalysis(self, src, trg, filter_set, at_k,
            source_confidence_threshold=None, target_confidence_threshold=None,
            order_by='ConfidenceWeightedDelta', limit=10, entity_key=None):

        base_query = '''
        SELECT
            eoa.*,
            ic_src.Confidence AS SourceInternalConfidence,
            ic_trg.Confidence AS TargetInternalConfidence,
            (
                ic_src.Confidence
                * ic_trg.Confidence
                * (1 - eoa.ENSimilarity)
            ) AS ConfidenceWeightedDelta,
            et.Term
        FROM
            EntityOverlapAnalysis AS eoa
        INNER JOIN
            EntityTerms AS et
        ON
            et.EntityKey = eoa.EntityKey
        INNER JOIN
            InternalConfidence AS ic_src
        ON
            ic_src.EntityKey = eoa.EntityKey
            AND ic_src.AtK = eoa.AtK
            AND ic_src.Source = eoa.Source
        INNER JOIN
            InternalConfidence AS ic_trg
        ON
            ic_trg.EntityKey = eoa.EntityKey
            AND ic_trg.AtK = eoa.AtK
            AND ic_trg.Source = eoa.Target
        WHERE
            eoa.Source=?
            AND eoa.Target=?
            AND eoa.FilterSet=?
            AND eoa.AtK=?
            AND et.Preferred=1
            {0}
        ORDER BY {1}
        LIMIT {2}
        '''

        args = [
            src,
            trg,
            filter_set,
            at_k
        ]

        if not (source_confidence_threshold is None):
            src_conf_cond = 'AND ic_src.Confidence >= ?'
            args.append(source_confidence_threshold)
        else:
            src_conf_cond = ''

        if not (target_confidence_threshold is None):
            trg_conf_cond = 'AND ic_trg.Confidence >= ?'
            args.append(target_confidence_threshold)
        else:
            trg_conf_cond = ''

        if not (entity_key is None):
            entity_key_cond = 'AND eoa.EntityKey = ?'
            args.append(entity_key)
        else:
            entity_key_cond = ''

        query = base_query.format(
            '{0} {1} {2}'.format(
                src_conf_cond,
                trg_conf_cond,
                entity_key_cond
            ),
            order_by,
            limit
        )

        self._cursor.execute(query, args)
        raw_rows = list(self._cursor)

        embedding_set_IDs = set()
        for row in raw_rows:
            embedding_set_IDs.add(row[0])
            embedding_set_IDs.add(row[1])
        embedding_sets = self.selectFromEmbeddingSets(ids=embedding_set_IDs)
        embedding_sets_by_ID = { e_set.ID: e_set for e_set in embedding_sets }

        for row in raw_rows:
            (
                source_ID,
                target_ID,
                filter_set,
                at_k,
                key,
                EN_similarity,
                source_confidence,
                target_confidence,
                CWD,
                preferred_term
            ) = row
            ret_obj = EntityOverlapAnalysis(
                source=embedding_sets_by_ID[source_ID],
                target=embedding_sets_by_ID[target_ID],
                filter_set=filter_set,
                at_k=at_k,
                key=key,
                source_confidence=source_confidence,
                target_confidence=target_confidence,
                EN_similarity=EN_similarity,
                CWD=CWD,
                string=preferred_term
            )
            yield ret_obj


    def selectFromInternalConfidence(self, src=None, at_k=None, key=None):
        query = '''
        SELECT
            *
        FROM
            InternalConfidence
        {0}{1}
        '''

        where_conds, args = [], []
        if not (src is None):
            where_conds.append('Source=?')
            args.append(src)
        if not (at_k is None):
            where_conds.append('AtK=?')
            args.append(at_k)
        if not (key is None):
            where_conds.append('EntityKey=?')
            args.append(key)

        if len(where_conds) > 0:
            where_conds = ' AND '.join(where_conds)
            query = query.format(
                'WHERE ',
                where_conds
            )
        else:
            query = query.format('', '')

        self._cursor.execute(query, args)
        raw_rows = list(self._cursor)

        embedding_set_IDs = set([
            row[0] for row in raw_rows
        ])
        embedding_sets = self.selectFromEmbeddingSets(ids=embedding_set_IDs)
        embedding_sets_by_ID = { e_set.ID: e_set for e_set in embedding_sets }

        for row in self._cursor:
            (
                source_ID,
                at_k,
                entity_key,
                confidence
            ) = row
            ret_obj = InternalConfidence(
                source=embedding_sets_by_ID[source_ID],
                at_k=at_k,
                key=entity_key,
                confidence=confidence
            )
            yield ret_obj


    def selectFromAggregateNearestNeighbors(self, src, trg, filter_set, key,
            neighbor_type=EmbeddingType.ENTITY, limit=10):

        query = '''
        SELECT
            ann.Source,
            anns.Target,
            anns.FilterSet,
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
            LEFT OUTER JOIN
                EntityTerms AS et_nbr
                ON
                    et_nbr.EntityKey = ann.NeighborKey
                    AND et_nbr.Preferred = 1
        WHERE
            ann.Source=?
            AND anns.Target=?
            AND anns.FilterSet=?
            AND ann.EntityKey=?
            AND ann.NeighborType=?
        ORDER BY ann.MeanDistance ASC
        LIMIT {0}
        '''.format(limit)

        args = [
            src,
            trg,
            filter_set,
            key,
            neighbor_type
        ]

        self._cursor.execute(query, args)
        raw_rows = list(self._cursor)

        embedding_set_IDs = set()
        for row in raw_rows:
            embedding_set_IDs.add(row[0])
            embedding_set_IDs.add(row[1])
        embedding_sets = self.selectFromEmbeddingSets(ids=embedding_set_IDs)
        embedding_sets_by_ID = { e_set.ID: e_set for e_set in embedding_sets }

        for row in self._cursor:
            (
                source_ID,
                target_ID,
                filter_set,
                entity_key,
                neighbor_key,
                mean_distance,
                query_term,
                neighbor_term
            ) = row
            ret_obj = AggregateNearestNeighbor(
                source=embedding_sets_by_ID[source_ID],
                target=embedding_sets_by_ID[target_ID],
                filter_set=filter_set,
                key=entity_key,
                string=query_term,
                neighbor_key=neighbor_key,
                neighbor_string=neighbor_term,
                mean_distance=mean_distance
            )
            yield ret_obj

    def selectAllIDsFromAggregateNearestNeighbors(self, src, trg, filter_set,
            neighbor_type=EmbeddingType.ENTITY):
        """Selects all neighbor sets for the given source and target corpus.
        Returned objects DO NOT contain query and neighbor string names for
        performance reasons."""

        query = '''
        SELECT
            ann.Source,
            anns.Target,
            anns.FilterSet,
            ann.EntityKey,
            ann.NeighborKey,
            ann.MeanDistance
        FROM
            AggregateNearestNeighbors AS ann
            INNER JOIN
                AggregateNearestNeighborSubsets AS anns
                ON
                    anns.NeighborID = ann.ID
        WHERE
            ann.Source=?
            AND anns.Target=?
            AND anns.FilterSet=?
            AND ann.NeighborType=?
        ORDER BY ann.MeanDistance ASC
        '''

        args = [
            src,
            trg,
            filter_set,
            neighbor_type
        ]

        self._cursor.execute(query, args)
        for row in self._cursor:
            (
                source,
                target,
                filter_set,
                entity_key,
                neighbor_key,
                mean_distance,
            ) = row
            ret_obj = AggregateNearestNeighbor(
                source=source,
                target=target,
                filter_set=filter_set,
                key=entity_key,
                string=None,
                neighbor_key=neighbor_key,
                neighbor_string=None,
                mean_distance=mean_distance
            )
            yield ret_obj

    def findAggregateNearestNeighborsMembership(self, key,
            neighbor_type=EmbeddingType.ENTITY):
        query = '''
        SELECT
            DISTINCT(Source)
        FROM
            AggregateNearestNeighbors
        WHERE
            EntityKey=?
        '''
        args = [key]

        self._cursor.execute(query, args)
        for row in self._cursor:
            (source,) = row
            yield source


    def selectFromEntityTerms(self, key, preferred=False):
        query = '''
        SELECT
            *
        FROM
            EntityTerms
        WHERE
            EntityKey=?
        '''

        args = [key]

        if preferred:
            query = '''
            {0}
                AND Preferred=1
            '''.format(query)

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

    def selectAllPreferredEntityNamesWithNeighbors(self):
        query = '''
        SELECT
            ann.EntityKey as EntityKey,
            et_query.Term as EntityName
        FROM
            EntityTerms AS et_query
            INNER JOIN
                AggregateNearestNeighbors AS ann
                ON
                    ann.EntityKey = et_query.EntityKey
        WHERE
            et_query.Preferred = 1
        '''

        self._cursor.execute(query)
        for row in self._cursor:
            (
                entity_key,
                term
            ) = row
            ret_obj = EntityTerm(
                entity_key=entity_key,
                term=term,
                preferred=True
            )
            yield ret_obj

    def selectFromEntityDefinitions(self, key):
        query = '''
        SELECT
            *
        FROM
            EntityDefinitions
        WHERE
            EntityKey=?
        '''

        args = [key]

        self._cursor.execute(query, args)
        for row in self._cursor:
            (
                entity_key,
                definition,
            ) = row
            ret_obj = EntityDefinition(
                entity_key=entity_key,
                definition=definition
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


    def selectFromAggregatePairwiseSimilarity(self, query_key, target, src=None):
        query = '''
        SELECT
            *
        FROM
            AggregatePairwiseSimilarity
        WHERE
            EntityKey=?
            AND NeighborKey=?
        '''

        args = [
            query_key,
            target
        ]

        if not (src is None):
            query = '''
            {0}
                AND Source=?
            '''.format(query)
            args.append(src)

        self._cursor.execute(query, args)
        raw_rows = list(self._cursor)

        embedding_set_IDs = set([
            row[0] for row in raw_rows
        ])
        embedding_sets = self.selectFromEmbeddingSets(ids=embedding_set_IDs)
        embedding_sets_by_ID = { e_set.ID: e_set for e_set in embedding_sets }

        for row in self._cursor:
            (
                source_ID,
                key,
                neighbor_key,
                mean_similarity,
                std_similarity
            ) = row
            ret_obj = AggregatePairwiseSimilarity(
                source=embedding_sets_by_ID[source_ID],
                key=key,
                neighbor_key=neighbor_key,
                mean_similarity=mean_similarity,
                std_similarity=std_similarity
            )
            yield ret_obj
