import pandas as pd
import os

def __get_organ_from_uberon( organ ):
    '''
    For full list, visit
    https://github.com/hubmapconsortium/search-api/blob/test-release/src/search-schema/data/definitions/enums/organ_types.yaml
    '''
   
    organs = {}
    organs['small intestine'] = 'UBERON:0002108' #small intestine
    organs['large intestine'] = 'UBERON:0000059' #large intestine
    organs['left kidney'] = 'UBERON:0004538' #left kidney
    organs['right kidney'] = 'UBERON:0004539' #right kidney
    organs['kidney (left)'] = 'UBERON:0004538' #left kidney
    organs['kidney (right)'] = 'UBERON:0004539' #right kidney
    organs['spleen'] = 'UBERON:0002106' #spleen    
    organs['thymus'] = 'UBERON:0002370' #thymus
    organs['heart'] = 'UBERON:0000948' #heart
    organs['LY01'] = 'UBERON:0000029' #lymph node
    organs['LY02'] = 'UBERON:0000029' #lymph node
    organs['LY03'] = 'UBERON:0000029' #lymph node
    organs['LY04'] = 'UBERON:0000029' #lymph node
    organs['LY05'] = 'UBERON:0000029' #lymph node
    organs['LY06'] = 'UBERON:0000029' #lymph node
    organs['LY07'] = 'UBERON:0000029' #lymph node
    organs['LY08'] = 'UBERON:0000029' #lymph node
    organs['LY09'] = 'UBERON:0000029' #lymph node
    organs['LY10'] = 'UBERON:0000029' #lymph node
    organs['LY11'] = 'UBERON:000029' #lymph node
    organs['LY11'] = 'UBERON:0000029' #lymph node
    organs['lymph node'] = 'UBERON:0000029' #lymph node
    organs['skin'] = 'UBERON:0002097' #skin
    organs['blood'] = 'UBERON:0000178' #blood 

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
