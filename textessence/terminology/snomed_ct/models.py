class Concept:
    ID = None
    descriptions = None
    definition = None

    def __init__(self, ID=None, descriptions=None, definition = None):
        self.ID = ID
        self.descriptions = [] if descriptions is None else descriptions
        self.definition = None

class Description:
    ID = None
    concept_ID = None
    term = None

    def __init__(self, ID=None, concept_ID=None, term=None):
        self.ID = ID
        self.concept_ID = concept_ID
        self.term = term

class Definition:
    ID = None
    concept_ID = None
    text = None

    def __init__(self, ID=None, concept_ID=None, text=None):
        self.ID = ID
        self.concept_ID = concept_ID
        self.text = text
