import pandas as pd
import os

def _build_dataframe( hubmap_id ):
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'tag:hubmapconsortium.org,2022:'
    headers = ['id_namespace', 'local_id', \
               'persistent_id', 'creation_time', \
               'abbreviation', 'name', \
               'description', 'has_time_series_data']
    df = pd.DataFrame(columns=headers)
    df = df.append({'id_namespace':id_namespace, \
                    'local_id':hubmap_id, \
                    'name':hubmap_id}, ignore_index=True)

    return df

def create_manifest( hubmap_id, output_directory ):
    filename = os.path.join( output_directory, 'collection.tsv' )
    df = _build_dataframe( hubmap_id )
    df.to_csv( filename, sep="\t", index=False)

    return True
