'''
'''

from hedgepig_logger import log
from . import semantic_types_and_groups
from ..terminology_data_models import CategoryMap

def buildSemanticTypeMap(
    mrconso=None,
    mrsty=None,
    semgroups=None,
    SABs=None,
    mode=None,
    use_SAB_code=False
):
    if SABs:
        SABs = set(SABs)
    mode = semantic_types_and_groups.UMLSMappingMode.parse(mode)
    use_SAB_code = str(use_SAB_code).strip().lower() == 'true'

    log.writeln('Reading CUIs and SAB codes...')
    log.writeln('  mrconso: %s' % mrconso)
    log.writeln('  SABs to filter to: {0}'.format(SABs))
    CUI_to_SAB_codes = semantic_types_and_groups.getCUIsAndSABCodes(mrconso, SABs)
    log.writeln('Read {1:,} total mappings for {0:,} unique CUIs.\n'.format(
        len(CUI_to_SAB_codes),
        sum([len(v) for v in CUI_to_SAB_codes.values()])
    ))

    log.writeln('Reading CUI-level semantic type mappings from %s...' % mrsty)
    category_mapping = semantic_types_and_groups.getCUITypes(mrsty, CUI_to_SAB_codes.keys())
    log.writeln('Read {1:,} total mappings for {0:,} unique CUIs.\n'.format(
        len(category_mapping),
        sum([len(v) for v in category_mapping.values()])
    ))
    
    if mode == semantic_types_and_groups.UMLSMappingMode.Group:
        log.writeln('Mapping CUI STYs to Semantic Groups using %s...' % semgroups)
        category_mapping = semantic_types_and_groups.mapCUITypesToGroups(category_mapping, semgroups)
        log.writeln('Mapped {0:,} unique CUIs to {1:,} total semantic groups.\n'.format(
            len(category_mapping),
            sum([len(v) for v in category_mapping.values()])
        ))
        allow_multiple = False
    else:
        allow_multiple = True

    if use_SAB_code:
        log.writeln('Remapping CUIs to SAB codes...')
        category_mapping = semantic_types_and_groups.remapCUITypesToSABCodes(category_mapping, CUI_to_SAB_codes)
        log.writeln('New mapping: {0:,} unique SAB codes to {1:,} total mappings.\n'.format(
            len(category_mapping),
            sum([len(v) for v in category_mapping.values()])
        ))

    category_map = CategoryMap(
        allow_multiple=allow_multiple
    )
    for (key, categories) in category_mapping.items():
        for category in sorted(categories):
            category_map.addMapping(
                key,
                category
            )

    return category_map
