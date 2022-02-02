from .umls_parsers import *

class UMLSMappingMode:
    STY = 'STY'
    Group = 'Group'

    @staticmethod
    def parse(value):
        if value.strip().lower() == UMLSMappingMode.STY.lower():
            return UMLSMappingMode.STY
        elif value.strip().lower() == UMLSMappingMode.Group.lower():
            return UMLSMappingMode.Group
        else:
            raise ValueError('UMLS mapping mode "{0}" is not configured'.format(value))


def getCUIsAndSABCodes(MRCONSO, SABs):
    if SABs:
        SAB_filter = lambda SAB: SAB in SABs
    else:
        SAB_filter = lambda SAB: True

    CUI_to_SAB_codes = {}
    with MRCONSOParser(MRCONSO) as parser:
        for record in parser:
            if SAB_filter(record.SAB):
                if not record.CUI in CUI_to_SAB_codes:
                    CUI_to_SAB_codes[record.CUI] = set()
                CUI_to_SAB_codes[record.CUI].add(record.CODE)
    return CUI_to_SAB_codes

def getCUITypes(MRSTY, CUIs):
    CUI_types = {}
    with MRSTYParser(MRSTY) as parser:
        for record in parser:
            if record.CUI in CUIs:
                if not record.CUI in CUI_types:
                    CUI_types[record.CUI] = set()
                CUI_types[record.CUI].add(record.STY)
    return CUI_types

def mapCUITypesToGroups(CUI_types, SemGroups):
    types_to_groups = {}
    with SemGroupsParser(SemGroups) as parser:
        for record in parser:
            types_to_groups[record.STY] = record.GROUP

    CUI_groups = {}
    for (CUI, STYs) in CUI_types.items():
        groups = set()
        for STY in STYs:
            groups.add(types_to_groups[STY])
        CUI_groups[CUI] = groups
    return CUI_groups

def remapCUITypesToSABCodes(CUI_types, CUI_to_SAB):
    SAB_types = {}
    for (CUI, types) in CUI_types.items():
        for SAB_code in CUI_to_SAB[CUI]:
            if not SAB_code in SAB_types:
                SAB_types[SAB_code] = set()
            SAB_types[SAB_code] = types.union(SAB_types[SAB_code])
    return SAB_types
