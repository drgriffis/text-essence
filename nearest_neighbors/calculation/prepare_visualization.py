import os
import json
import numpy as np
import pandas as pd
from io import StringIO
from sklearn.manifold import TSNE
import configparser
import pyemblib
from hedgepig_logger import log
from ..database import EmbeddingNeighborhoodDatabase
from . import moving_scatterplot as ms

AT_K = 5

def build_frame(embedding, embedding_set, db, labels, confidence_threshold=0.0, num_to_plot=None, num_neighbors=10):
    """
    Builds a ScatterplotFrame using the given embedding object. 
    
    Args:
        embedding: A pyemblib embedding object
        embedding_set: An EmbeddingSet object that can be used to retrieve
            neighbors and confidence values
        db: An EmbeddingNeighborhoodDatabase
        labels: A dictionary mapping entity IDs to string labels 
        confidence_threshold: Filter by confidence
        num_to_plot: Number of highest-confidence entities to plot (or None to
            plot all)
        num_neighbors: Number of neighbors to retrieve
    
    Returns: A ScatterplotFrame containing the following keys:
     - id: The ID/query key of the entity
     - x: x position of the entity in a TSNE projection
     - y: y position of the entity in a TSNE projection
     - highlight: list of IDs of nearest neighbors for the entity 
     - confidence: confidence of the given entity in this embedding
     - color: a label that groups the entities for coloring in the visualization
     - hoverText: The preferred name of the entity
    """
    ids = set(embedding.keys())
    points = []
    num_filtered = {}
    
    # Get all nearest neighbors for this corpus
    log.writeln("Getting nearest neighbors...")
    filter_set='.HC_{0}'.format(embedding_set.name)
    neighbor_rows = db.selectAllIDsFromAggregateNearestNeighbors(
        embedding_set, embedding_set, filter_set
    )
    neighbor_sets = {}
    for row in neighbor_rows:
        if len(neighbor_sets.get(row.key, [])) >= num_neighbors: continue
        if row.neighbor_key not in labels: continue
        neighbor_sets.setdefault(row.key, []).append(row.neighbor_key)
    log.writeln("Neighbors for {} IDs".format(len(neighbor_sets)))

    for id_val in ids:
        if id_val not in labels:
            num_filtered["disallowed_group"] = num_filtered.get("disallowed_group", 0) + 1
            continue
        
        # Get nearest neighbors for the entity
        if id_val not in neighbor_sets:
            num_filtered["no_neighbors"] = num_filtered.get("no_neighbors", 0) + 1
            continue
        neighbors = neighbor_sets[id_val]

        # Get confidence for the ID
        confidence_rows = db.selectFromInternalConfidence(
            src=embedding_set,
            at_k=AT_K,
            key=id_val
        )
        if not confidence_rows:
            num_filtered["no_confidence"] = num_filtered.get("no_confidence", 0) + 1
            continue
        
        # Remove if confidence is below confidence threshold
        confidence = next(confidence_rows).confidence
        if confidence < confidence_threshold:
            num_filtered["low_confidence"] = num_filtered.get("low_confidence", 0) + 1
            continue

        # Get preferred name of the entity
        name_rows = db.selectFromEntityTerms(id_val, preferred=True)
        if not name_rows:
            num_filtered["no_name"] = num_filtered.get("no_name", 0) + 1
            continue
        preferred_name = next(name_rows).term
                
        points.append({
            "id": id_val,
            "color": labels[id_val],
            "hoverText": preferred_name,
            "confidence": confidence,
            "highlight": neighbors
        })

    log.writeln("{} in embedding, filtered: {}, remaining: {}".format(
        len(ids), num_filtered, len(points)))
    
    if num_to_plot is not None and len(points) > num_to_plot:
        # Sort by confidence and filter
        points = sorted(points,
                        key=lambda x: x["confidence"],
                        reverse=True)[:num_to_plot]
        log.writeln(("Keeping {} highest-confidence points "
                     "(lowest confidence: {:.3f})").format(
                         len(points),
                         points[-1]["confidence"] if points else "nan"))
    
    # Build ScatterplotFrame
    frame = ms.ScatterplotFrame(points)

    # Compute TSNE and add to the frame
    hi_d = np.vstack([embedding[id_val] for id_val in frame.get_ids()])
    lo_d = TSNE(metric='cosine', n_iter=2000).fit_transform(hi_d)
    frame.set_mat(["x", "y"], lo_d)
    
    return frame

def choose_base_frame(frames):
    """
    Returns the index of the frame that should be used to align the embeddings.
    This is the frame with the maximum minimum overlap in IDs present between
    all of the frames.
    """
    max_overlap = 0
    best_frame = 0
    
    for i, frame in enumerate(frames):
        min_overlap = min(len(set(frame.get_ids()) & set(other_frame.get_ids())) for other_frame in frames)
        if min_overlap >= max_overlap:
            max_overlap = min_overlap
            best_frame = i
        
    return best_frame

def align_frames(frames):
    """
    Aligns the given ScatterplotFrames and writes the aligned coordinates to the
    'aligned_x' and 'aligned_y' fields of each frame.
    """
    base_frame_idx = choose_base_frame(frames)
    log.writeln('Aligning to frame {}'.format(base_frame_idx))
    base_frame = frames[base_frame_idx]

    for i, frame in enumerate(frames):
        best_proj = ms.align_projection(base_frame, frame)
        frame.set_mat(["aligned_x", "aligned_y"], best_proj[:,:2])

def write_visualization_file(frames, corpora, out_path, x_key, y_key):
    """
    Writes out a JSON file with the given visualization data.
    """
    data = [frame.to_viewer_dict(x_key=x_key, 
                                 y_key=y_key,
                                 additional_fields={
                                     "color": lambda _, item: item["color"],
                                     "highlight": lambda _, item: item["highlight"],
                                     "confidence": lambda _, item: item["confidence"],
                                     "hoverText": lambda _, item: item["hoverText"]
                                 })
            for frame in frames]
    with open(out_path, "w") as file:
        json.dump(ms.round_floats({
            "data": data,
            "frameLabels": corpora,
            "previewMode": "neighborSimilarity"
        }, 4), file)
        

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog')
        parser.add_option('-i', '--input', dest='input_base',
            help='(required) base path for embeddings')
        parser.add_option('-g', '--group', dest='embedding_group',
            help='(required) group name for embedding sets in the database')
        parser.add_option('-c', '--config', dest='configf',
            default='config.ini')
        parser.add_option('-b', '--labels', dest='labels_file',
                          help='path to a CSV file with two columns (entity ID and label)')
        parser.add_option('-f', '--filter-labels', dest='allowed_labels',
                          default='',
                          help=('Comma-separated list of labels to '
                                'visualize (if empty, allows all labels)'))
        parser.add_option('-l', '--logfile', dest='logfile',
            help='name of file to write log contents to (empty for stdout)',
            default=None)
        (options, args) = parser.parse_args()

        assert options.input_base, "Input base path required"
        assert options.embedding_group, "Embedding set group required"
        assert options.labels_file, "Labels path required"
        return args, options

    embedfs, options = _cli()
    log.start(options.logfile)
    log.writeConfig([
        ('Base path for embeddings', options.input_base),
        ('Embedding set group', options.embedding_group),
        ('Labels csv file', options.labels_file),
        ('Allowed labels', options.allowed_labels),
        ('Configuration file', options.configf),
    ], 'Generation of visualization file')


    log.writeln('Reading configuration file from %s...' % options.configf)
    config = configparser.ConfigParser()
    config.read(options.configf)
    analysis_config = config['PairedNeighborhoodAnalysis']
    visualization_config = config['Visualization']
    hc_threshold = float(analysis_config['HighConfidenceThreshold'])
    num_neighbors = int(analysis_config['NumNeighborsToShow'])
    log.writeln('Done.\n')

    log.writeln('Loading embedding neighborhood database...')
    db = EmbeddingNeighborhoodDatabase(analysis_config['DatabaseFile'])
    log.writeln('Database ready.\n')
    
    log.writeln('Loading labels file...')
    labels_df = pd.read_csv(options.labels_file, dtype='str')
    labels_df = labels_df.set_index(labels_df.columns[0])
    label_col = labels_df.columns[0]
    log.writeln('Using field "{}" as label.\n'.format(label_col))
    
    if options.allowed_labels:
        allowed_labels = options.allowed_labels.split(",")
        log.writeln('Allowed labels: {}'.format(allowed_labels))
        labels_df = labels_df[labels_df[label_col].isin(allowed_labels)]
    labels_df = labels_df[~labels_df.index.duplicated(keep='first')]
    labels = labels_df[label_col].to_dict()

    frames = []
    corpora = analysis_config['CorpusOrdering'].split(',')
    for corpus in corpora:
        log.writeln('Loading embedding for {}...'.format(corpus))
        emb_set = db.getOrCreateEmbeddingSet(name=corpus, group_name=options.embedding_group)
        emb_path = os.path.join(
            options.input_base,
            visualization_config['EmbeddingFilePattern'].format(CORPUS=corpus))
        embedding = pyemblib.read(emb_path,
                                  mode=visualization_config['EmbeddingFormat'],
                                  errors='replace')

        t = log.startTimer('Building frame and running TSNE...')
        frame = build_frame(embedding, emb_set, db, labels,
                            num_to_plot=int(visualization_config["NumEntitiesPerFrame"]),
                            confidence_threshold=hc_threshold,
                            num_neighbors=num_neighbors)
        frames.append(frame)
        log.stopTimer(t, 'Done in {0:.2f}s.')
        
    # Align the frames and write
    log.writeln('Aligning frames...')
    align_frames(frames)
    log.writeln('Writing visualization file...')
    write_visualization_file(frames,
                             corpora,
                             visualization_config['OutputFile'],
                             "aligned_x", "aligned_y")
    log.writeln('Done.')
    
    log.stop()
