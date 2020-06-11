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

    def __init__(self, source, target, at_k, key, source_confidence, target_confidence, EN_similarity):
        self.source = source
        self.target = target
        self.at_k = at_k
        self.key = key
        self.source_confidence = source_confidence
        self.target_confidence = target_confidence
        self.EN_similarity = EN_similarity
