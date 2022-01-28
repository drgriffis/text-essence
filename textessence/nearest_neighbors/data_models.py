class EmbeddingSetGroup:
    ID = None
    short_name = None
    display_title = None

    def __init__(self, short_name, display_title=None, ID=None):
        self.ID = ID
        self.short_name = short_name
        self.display_title = display_title

class EmbeddingSet:
    ID = None
    group = None
    name = None
    ordering = None

    def __init__(self, group, name, ordering, ID=None):
        self.ID = ID
        self.group = group
        self.name = name
        self.ordering = ordering

class EntityOverlapAnalysis:
    source = None
    target = None
    filter_set = None
    at_k = None
    key = None
    source_confidence = None
    target_confidence = None
    EN_similarity = None         ## TODO: change this field name to Overlap
    CWD = None
    string = None

    def __init__(self, source, target, filter_set, at_k, key, source_confidence=None, target_confidence=None, EN_similarity=None, CWD=None, string=None):
        self.source = source
        self.target = target
        self.filter_set = filter_set
        self.at_k = at_k
        self.key = key
        self.source_confidence = source_confidence
        self.target_confidence = target_confidence
        self.EN_similarity = EN_similarity
        self.CWD = CWD
        self.string = string

class InternalConfidence:
    source = None
    at_k = None
    key = None
    confidence = None

    def __init__(self, source, at_k, key, confidence):
        self.source = source
        self.at_k = at_k
        self.key = key
        self.confidence = confidence

class AggregateNearestNeighbor:
    source = None
    target = None
    filter_set = None
    key = None
    string = None
    neighbor_key = None
    neighbor_string = None
    mean_distance = None

    def __init__(self, source, target, filter_set, key, neighbor_key, mean_distance, string=None, neighbor_string=None):
        self.source = source
        self.target = target
        self.filter_set = filter_set
        self.key = key
        self.string = string
        self.neighbor_key = neighbor_key
        self.neighbor_string = neighbor_string
        self.mean_distance = mean_distance

class EntityTerm:
    entity_key = None
    term = None
    preferred = None

    def __init__(self, entity_key, term, preferred):
        self.entity_key = entity_key
        self.term = term
        self.preferred = preferred

class EntityDefinition:
    entity_key = None
    definition = None

    def __init__(self, entity_key, definition):
        self.entity_key = entity_key
        self.definition = definition

class AggregatePairwiseSimilarity:
    source = None
    key = None
    neighbor_key = None
    mean_similarity = None
    std_similarity = None

    def __init__(self, source, key, neighbor_key, mean_similarity, std_similarity):
        self.source = source
        self.key = key
        self.neighbor_key = neighbor_key
        self.mean_similarity = mean_similarity
        self.std_similarity = std_similarity
