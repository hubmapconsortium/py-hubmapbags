import pandas as pd
import os

def _build_dataframe( data_provider ):
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'tag:hubmapconsortium.org,2022:'
    headers = ['parent_project_id_namespace','parent_project_local_id','child_project_id_namespace','child_project_local_id']
    df = pd.DataFrame(columns=headers)

    parent_local_id = 'HuBMAP'
    parent_local_description = 'Human BioMolecular Atlas Program'
    df = df.append({'parent_project_id_namespace':id_namespace, \
                    'parent_project_local_id':parent_local_id, \
                    'child_project_id_namespace':id_namespace, \
                    'child_project_local_id':data_provider}, ignore_index=True)
  
    return df

def create_manifest( data_provider, output_directory ):
    filename = os.path.join( output_directory, 'project_in_project.tsv' )
    df = _build_dataframe( data_provider )
    df.to_csv( filename, sep="\t", index=False)

    return True
