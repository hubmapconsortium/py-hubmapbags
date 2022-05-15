import pandas as pd
import os

def _build_dataframe( project_id, donor_id ):
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'tag:hubmapconsortium.org,2022:'
    headers = ['id_namespace', 'local_id', 'project_id_namespace', 'project_local_id', 'persistent_id', 'creation_time', 'granularity']
    df = pd.DataFrame(columns=headers)
    df = df.append({'id_namespace':id_namespace, \
                     'local_id':donor_id, \
                     'project_id_namespace':id_namespace, \
                     'project_local_id':project_id, \
                     'persistent_id':donor_id, \
                     'granularity':'cfde_subject_granularity:0'}, ignore_index=True)

    return df

def create_manifest( project_id, donor_id, output_directory ):
    filename = os.path.join( output_directory, 'subject.tsv' )
    df = _build_dataframe( project_id, donor_id )
    df.to_csv( filename, sep="\t", index=False)

    return True
