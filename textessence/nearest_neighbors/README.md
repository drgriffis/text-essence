# How to calculate the stability (internal confidence) measure

This is a brief walkthrough of how to calculate internal confidence/stability for each entry in a set of embedding replicates from a given corpus.

## Step 0: Train embeddings

We will assume the following:

- You have already trained embeddings using an external toolkit;
- Embeddings are stored in word2vec binary format;
- You have 10 replicates of your embeddings (i.e., different sets of embeddings trained using the same algorithm on the same data but with different random seeds)
- Embeddings are stored in `data/embeddings.${i}.bin`, where `${i}` is the replicate identifier (e.g. `embeddings.1.bin`)

## Step 1: Calculate nearest neighbours

The first step is to calculate the nearest neighbours for each term in each individual embedding replicate.

_NB if you have 10 replicates, you will therefore calculate 10 sets of nearest neighbours._

For replicate `embeddings.1.bin` you would do so as follows (command-line parameters split across multiple lines for readability):
```bash
python -m textessence.nearest_neighbors.calculation.get_nearest_neighbors
  data/embeddings.1.bin
  -t 8                                         # Using 8 threads for parallel calculation
  -o data/embeddings.1.neighbors               # Output neighbours file
  --vocab data/embeddings.1.neighbors.vocab    # Vocabulary file mapping neighbour indices to terms
  --embedding-mode bin                         # Use 'txt' if in text format
  --with-distances                             # Include distances to neighbours in output file
  -l data/embeddings.1.neighbors.log           # Log file for execution
```

To repeat this step for embedding replicates 2-10, execute the above command using `embeddings.2` in place of `embeddings.1`, and so forth.

## Step 2: Managing configuration settings

The stability calculation relies on several settings within the `[PairedNeighborhoodAnalysis]` section of the root `config.ini` file in the `textessence` repository.

For our running example, we would use the following settings:
```
[PairedNeighborhoodAnalysis]

# DatabaseFile is used to store neighbors for later analysis (even if not used)
DatabaseFile = data/neighborhood_analysis.db

# NeighborFilePattern maps to the neighbours calculated in Step 1
# SRC is the prefix of the embedding file names
# SRC_RUN is a placeholder for the index of the embedding replicates
NeighborFilePattern = data/{SRC}.{SRC_RUN}.neighbors

# NeighborVocabFilePattern maps to the corresponding vocabulary map for each neighbors file
# Uses the same settings as NeighborFilePattern
NeighborVocabFilePattern = data/{SRC}.{SRC_RUN}.neighbors.vocab
```

## Step 3: Calculating stability

The final step is to combine the neighbours from all the embedding replicates to calculate stability/confidence.

Normally this stores the confidence values in a Sqlite database for later re-use, however you have the option to dump confidence values directly to an output CSV.

Executing this calculation requires a `-g` group name parameter to distinguish between different analysis sets in the database; choose whatever name you prefer!

For our running example this would be executed as follows (command-line parameters again split across multiple lines):

```bash
python -m textessence.nearest_neighbors.analysis.internal_confidence
  -g my_analysis                    # only used for storing in the database
  -s embeddings                     # the embedding file prefix, matches to SRC in the configurations in Step 2
  --dump data/embeddings.confidence # output CSV file with per-term confidence values
  -l data/embeddings.confidence.log # log file for code execution
```
