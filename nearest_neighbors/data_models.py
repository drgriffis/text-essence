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

    def __init__(self, source, filter_set, at_k, key, confidence):
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
