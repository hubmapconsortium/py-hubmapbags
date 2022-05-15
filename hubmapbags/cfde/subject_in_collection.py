import pandas as pd
import os

def _build_dataframe( donor_id, hubmap_id ):
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'tag:hubmapconsortium.org,2022:'
    headers = ['subject_id_namespace', 'subject_local_id', 'collection_id_namespace', 'collection_local_id']
    df = pd.DataFrame(columns=headers)
    df = df.append({'subject_id_namespace':id_namespace, \
                     'subject_local_id':donor_id, \
                     'collection_id_namespace':id_namespace, \
                     'collection_local_id':hubmap_id}, ignore_index=True)

    return df

def create_manifest( donor_id, hubmap_id, output_directory ):
    filename = os.path.join( output_directory, 'subject_in_collection.tsv' )
    df = _build_dataframe( donor_id, hubmap_id )
    df.to_csv( filename, sep="\t", index=False)

    return True
