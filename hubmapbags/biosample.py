import pandas as pd
import yaml
from urllib.request import urlopen
import os

def __get_organ_from_uberon( organ ):
    '''
    For full list, visit
    https://github.com/hubmapconsortium/search-api/blob/test-release/src/search-schema/data/definitions/enums/organ_types.yaml
    '''

    url = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/master/src/search-schema/data/definitions/enums/organ_types.yaml'
    with urlopen(url) as f:
        tbl = yaml.safe_load(f)

    organs = {}
    for key in tbl:
        if 'iri' in tbl[key]:
            s = tbl[key]['iri']
            oberon_entry = s[s.rfind('/')+1:]
            organs[key] = oberon_entry
            if 'description' in tbl[key]:
                desc = tbl[key]['description']
                organs[desc] = oberon_entry
                organs[desc.lower()] = oberon_entry
            if key == 'LK':
                organs['left kidney'] = oberon_entry
            elif key == 'RK':
                organs['right kidney'] = oberon_entry
    for i in range(1, 12):
        organs["LY%02d"%i] = organs["LY"]

    from pprint import pprint
    print('ORGAN TABLE FOLLOWS')
    pprint(organs)
    return organs[organ]

def _build_dataframe( biosample_id, data_provider, organ ):
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'tag:hubmapconsortium.org,2022:'
    headers = ['id_namespace', 'local_id', \
               'project_id_namespace', 'project_local_id', \
               'persistent_id', 'creation_time', \
               'assay_type', 'anatomy']
    df = pd.DataFrame(columns=headers)
    df = df.append({'id_namespace':id_namespace, \
       'local_id':biosample_id, \
       'project_id_namespace':id_namespace, \
       'project_local_id':data_provider, \
       'anatomy': __get_organ_from_uberon(organ)}, ignore_index=True)

    return df

def create_manifest( biosample_id, data_provider, organ, output_directory ):
    filename = os.path.join( output_directory, 'biosample.tsv' )
    df = _build_dataframe( biosample_id, data_provider, organ )
    df.to_csv( filename, sep="\t", index=False)

    return True
