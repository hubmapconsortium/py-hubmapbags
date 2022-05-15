import pandas as pd
import os

def _build_dataframe( collection_id, project_id ):
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'tag:hubmapconsortium.org,2022:'
    headers = ['collection_id_namespace', 'collection_local_id', \
               'project_id_namespace', 'project_local_id']
    df = pd.DataFrame(columns=headers)
    df = df.append({'collection_id_namespace':id_namespace, \
                    'collection_local_id':collection_id, \
                    'project_id_namespace':id_namespace, \
                    'project_local_id':project_id}, ignore_index=True)

    return df

def create_manifest( collection_id, project_id, output_directory ):
    filename = os.path.join( output_directory, 'collection_defined_by_project.tsv' )
    df = _build_dataframe( collection_id, project_id )
    df.to_csv( filename, sep="\t", index=False)

    return True
