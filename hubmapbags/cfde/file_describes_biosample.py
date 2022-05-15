import pandas as pd
from pathlib import Path
from shutil import rmtree
import datetime
import time
import os
import pickle
import mimetypes

def _build_dataframe():
    '''
    Build a dataframe with minimal information for this entity.
    '''

    id_namespace = 'tag:hubmapconsortium.org,2022:'
    headers = ['file_id_namespace', \
               'file_local_id', \
               'biosample_id_namespace', \
               'biosample_local_id']

    df = pd.DataFrame( columns = headers )

    return df

def create_manifest( output_directory ):
    filename = os.path.join( output_directory, 'file_describes_biosample.tsv' )
    df = _build_dataframe()
    df.to_csv( filename, sep="\t", index=False)
    
    return True

