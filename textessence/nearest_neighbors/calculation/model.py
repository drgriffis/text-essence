'''
'''

import numpy as np
import tensorflow as tf
import multiprocessing as mp

class MultiNearestNeighbors:
    
    def __init__(self, session, embed_arrays):
        self._session = session
        self._prints = []

        embed_shapes = [embed_array.shape for embed_array in embed_arrays]
        self._number_of_embeddings = len(embed_arrays)
        self._build(embed_shapes)

        self._session.run(tf.global_variables_initializer())

        # fill the (static) embedding matrices
        for i in range(len(embed_arrays)):
            # unit norm the embedding array
            embed_array = np.array([
                vec / np.linalg.norm(vec)
                    for vec in embed_arrays[i]
            ])
            self._session.run(
                self._embed_matrices[i].assign(
                    self._embed_phs[i]
                ),
                feed_dict={self._embed_phs[i]: embed_array}
            )

    def _build(self, emb_shapes):
        self._sample_indices = tf.placeholder(
            shape=[None,],
            dtype=tf.int32
        )
        self._sample_embeds = [
            tf.placeholder(
                shape=[None,emb_shapes[i][1]],
                dtype=tf.float32
            ) for i in range(len(emb_shapes))
        ]

        self._embed_phs = [
            tf.placeholder(
                shape=emb_shapes[i],
                dtype=tf.float32
            ) for i in range(len(emb_shapes))
        ]
        self._embed_matrices = [
            tf.Variable(
                tf.constant(0.0, shape=emb_shapes[i]),
                trainable=False
            ) for i in range(len(emb_shapes))
        ]
        self._sample_points = [
            tf.gather(
                self._embed_matrices[i],
                self._sample_indices
            ) for i in range(len(emb_shapes))
        ]

        self._indexed_sample_distances = [
            self._distance(self._sample_points[i], self._embed_matrices[i])
                for i in range(len(emb_shapes))
        ]
        self._embed_sample_distances = [
            self._distance(
                tf.nn.l2_normalize(self._sample_embeds[i], 1),
                self._embed_matrices[i]
            )
                for i in range(len(emb_shapes))
        ]

    def _distance(self, a, b):
        # first, L2-norm both inputs
        #normed_a = tf.nn.l2_normalize(a, 1)
        #normed_b = tf.nn.l2_normalize(b, 1)
        normed_a = a
        normed_b = b   # already unit-normed
        # get full pairwise distance matrix
        pairwise_distance = 1 - tf.matmul(normed_a, normed_b, transpose_b=True)
        return pairwise_distance

    def _print(self, *nodes):
        for n in nodes:
            if type(n) is tuple and len(n) == 2:
                self._prints.append(tf.Print(0, [n[0]], message=n[1], summarize=100))
            else:
                self._prints.append(tf.Print(0, [n], summarize=100))

    def _exec(self, nodes, feed_dict=None):
        all_nodes = [p for p in self._prints]
        all_nodes.extend(nodes)
        outputs = self._session.run(all_nodes, feed_dict=feed_dict)
        return outputs[len(self._prints):]

    def nearestNeighbors(self, batch_input, indices=True, top_k=None, no_self=True, with_distances=False):
        # get the pairwise distances for this batch for each set of embeddings
        all_distances = []
        for i in range(self._number_of_embeddings):
            if indices:
                (pairwise_distances,) = self._exec([
                        self._indexed_sample_distances[i]
                    ],
                    feed_dict = {
                        self._sample_indices: batch_input
                    }
                )
            else:
                (pairwise_distances,) = self._exec([
                        self._embed_sample_distances[i]
                    ],
                    feed_dict = {
                        self._sample_embeds[i]: batch_input[i]
                            for i in range(len(self._sample_embeds))
                    }
                )
            all_distances.append(pairwise_distances)

        # average the distances across all sets of embeddings
        all_distances = np.array(all_distances)
        averaged_distances = np.mean(all_distances, axis=0)
        assert len(averaged_distances) == len(pairwise_distances)

        nearest_neighbors = []
        if indices:
            itr = range(len(batch_input))
        else:
            itr = range(len(batch_input[0]))
        for i in itr:
            distance_vector = averaged_distances[i]
            sorted_neighbors = np.argsort(distance_vector)
            # if skipping the query, remove it from the neighbor list
            # (should be in the 0th position; if it's not, just move on)
            if no_self: 
                if sorted_neighbors[0] == batch_input[i]: sorted_neighbors = sorted_neighbors[1:]
            # if restricting to top k, do so here
            if top_k is None:
                kept_neighbors = sorted_neighbors
            else:
                kept_neighbors = sorted_neighbors[:top_k]
            # if including distance, pull those for the indices being kept
            if with_distances:
                kept_neighbors = [
                    (ix, distance_vector[ix])
                        for ix in kept_neighbors
                ]
            nearest_neighbors.append(kept_neighbors)
        return nearest_neighbors


class NearestNeighbors(MultiNearestNeighbors):
    
    def __init__(self, session, embed_array):
        super().__init__(session, [embed_array])
