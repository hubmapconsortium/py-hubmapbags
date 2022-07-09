import pandas as pd
from pathlib import Path
from shutil import rmtree
import datetime
import time
import os
import mimetypes
import pickle

def _build_dataframe( hubmap_id, directory ):
	'''
	Build a dataframe with minimal information for this entity.
	'''

	id_namespace = 'tag:hubmapconsortium.org,2022:'

	#name of the columns (order is important for validation)
	headers = ['file_id_namespace', \
		'file_local_id', \
		'collection_id_namespace', \
		'collection_local_id']

	#name of the file with checksums and file info
	temp_file = directory.replace('/','_').replace(' ','_') + '.pkl'

	if Path( temp_file ).exists():
		print('Temporary file ' + temp_file + ' found. Loading df into memory.')
		with open( temp_file, 'rb' ) as file:
			df = pickle.load(file)

		#remove unnecesary columns
		df = df.drop(columns=['project_id_namespace', 'project_local_id', \
			'persistent_id', 'creation_time', 'size_in_bytes', 'dbgap_study_id', \
			'uncompressed_size_in_bytes', 'sha256', 'md5', 'filename', \
			'file_format', 'data_type', 'assay_type', 'mime_type', 'sha256', \
			'compression_format','bundle_collection_id_namespace','bundle_collection_local_id'])

		df['collection_id_namespace']=id_namespace
		df = df.rename(columns={'id_namespace': 'file_id_namespace', \
			'local_id':'file_local_id'}, errors ="raise")
		df['collection_local_id'] = hubmap_id
		df[['file_id_namespace', 'file_local_id', 'collection_id_namespace', 'collection_local_id']]
	else:
		df = []

	return df[df['file_local_id'].str.contains('metadata.tsv')]

def create_manifest( hubmap_id, directory, output_directory ):
    filename = os.path.join( output_directory, 'file_describes_collection.tsv' )
    temp_file = directory.replace('/','_').replace(' ','_') + '.pkl'
    if not Path(directory).exists() and not Path(temp_file).exists():
        print('Data directory ' + directory + ' does not exist. Temp file was not found either.')
        return False
    else:
        if Path(temp_file).exists():
            print('Temp file ' + temp_file + ' found. Continuing computation.')
        df = _build_dataframe( hubmap_id, directory )
        df.to_csv( filename, sep="\t", index=False)
        return True
