import pandas as pd
import os

def _build_dataframe( data_provider ):
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'tag:hubmapconsortium.org,2022:'
    headers = ['id_namespace', 'local_id', 'persistent_id', 'creation_time', 'abbreviation', 'name', 'description']
    df = pd.DataFrame(columns=headers)

    parent_local_id = 'HuBMAP'
    parent_local_description = 'Human BioMolecular Atlas Program'
    df = df.append({'id_namespace':id_namespace, \
                    'local_id':parent_local_id, \
                    'abbreviation':parent_local_id, \
                    'name':parent_local_description}, ignore_index=True)

    df = df.append({'id_namespace':id_namespace, \
                    'local_id':data_provider, \
                    'abbreviation':data_provider.replace(' ','_'), \
                    'name':data_provider}, ignore_index=True)
  
    return df

def create_manifest( data_provider, output_directory ):
    filename = os.path.join( output_directory, 'project.tsv' )
    df = _build_dataframe( data_provider )
    df.to_csv( filename, sep="\t", index=False)

    return True
