SNOMED_CT = 'SNOMED_CT'

def parse(value):
    if value.strip().lower() == SNOMED_CT.lower():
        return SNOMED_CT
    else:
        raise ValueError('Terminology source "{0}" is not yet configured.'.format(value))
