class EntityOverlapAnalysis:
    source = None
    target = None
    at_k = None
    key = None
    source_confidence = None
    target_confidence = None
    EN_similarity = None
    CWD = None
    CWS = None
    string = None

    def __init__(self, source, target, at_k, key, source_confidence, target_confidence, EN_similarity, CWD=None, CWS=None, string=None):
        self.source = source
        self.target = target
        self.at_k = at_k
        self.key = key
        self.source_confidence = source_confidence
        self.target_confidence = target_confidence
        self.EN_similarity = EN_similarity
        self.CWD = CWD
        self.CWS = CWS
        self.string = string

class AggregateNearestNeighbor:
    source = None
    target = None
    key = None
    string = None
    neighbor_key = None
    neighbor_string = None
    mean_distance = None

    def __init__(self, source, target, key, neighbor_key, mean_distance, string=None, neighbor_string=None):
        self.source = source
        self.target = target
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
