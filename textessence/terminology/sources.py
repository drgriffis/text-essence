class TerminologySources:
    SNOMED_CT = 'SNOMED_CT'

    @staticmethod
    def parse(value):
        if value.strip().lower() == TerminologySources.SNOMED_CT.lower():
            return TerminologySources.SNOMED_CT
        else:
            raise ValueError('Terminology source "{0}" is not yet configured.'.format(value))
