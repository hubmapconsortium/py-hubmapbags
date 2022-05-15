import pandas as pd
import os

def _build_dataframe():
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'HuBMAP'
    headers = ['id', 'abbreviation', 'name', 'description']
    df = pd.DataFrame(columns=headers)
    df = df.append({'id':'HuBMAP', \
           'abbreviation':'HuBMAP', \
           'name':'HuBMAP', \
           'description':'Human BioMolecular Atlas Program'}, ignore_index=True)

    return df

def create_manifest( output_directory ):
    filename = os.path.join( output_directory, 'id_namespace.tsv' )
    df = _build_dataframe()
    df.to_csv( filename, sep="\t", index=False)

    return True
