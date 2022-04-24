import pandas as pd
import os

def _build_dataframe():
    '''
    Build a dataframe with minimal information for this entity.
    '''

    headers = ['id','contact_email', 'contact_name', 'project_id_namespace', 'project_local_id', 'dcc_abbreviation', 'dcc_name', 'dcc_description', 'dcc_url'] 
    df = pd.DataFrame(columns=headers)
    df = df.append({ 'id':'cfde_registry_dcc:HuBMAP', \
        'project_id_namespace':'tag:hubmapconsortium.org,2022:', \
        'project_local_id':'HuBMAP', \
        'contact_email':'cfde-submissions@hubmapconsortium.org',\
        'contact_name':'Ivan Cao-Berg', \
        'dcc_abbreviation':'HuBMAP', \
        'dcc_name':'HuBMAP', \
        'dcc_description':'Human BioMolecular Atlas Program',\
        'dcc_url':'http://portal.hubmapconsortium.org'}, ignore_index=True)

    return df

def create_manifest( output_directory ):
    filename = os.path.join( output_directory, 'dcc.tsv' )
    df = _build_dataframe()
    df.to_csv( filename, sep="\t", index=False)

    return True
