import glob
import hubmapbags
import subprocess
from os.path import exists
from tabulate import tabulate 
import pandas as pd
from datetime import date
from os.path import basename

token=''
output_directory = 'reports'

today = date.today()
filename = 'pickle-report-' + str(today).replace('-','') + '.tsv'

def compute_number_of_files( directory ):
	command = 'find "' + directory + '" -type f | wc -l'
	answer = subprocess.check_output( command, shell=True )
	return str(answer.decode( 'utf-8' ).replace('\n','').replace('"',''))

def get_number_of_records( filename ):
	if exists( filename ):
		df = pd.read_pickle( filename )
		return int(len(df))
	else:
		return None

def get_dataset_info( hubmap_id, instance='prod', token=None ):
	try:
		datasets = hubmapbags.magic.__extract_dataset_info_from_db( hubmap_id, token=token, instance=instance )
		for dataset in datasets.iterrows():
			dataset = dataset[1]
			return dataset
	except:
		print(hubmap_id)
		return None

def generate_directory( dataset ):
	return dataset['full_path']

def generate_pickle_filename( dataset ):
	data_directory = dataset['full_path']
	pickle_filename = data_directory.replace('/','_').replace(' ','_') + '.pkl'
	print( pickle_filename )
	if exists(pickle_filename):
		return pickle_filename
	else:
		return ''
	
def get_number_of_files( directory ):
	return None

assays = ['AF', 'ATACseq-bulk', 'cell-dive', 'CODEX', 'DART-FISH', 'IMC2D', 'IMC3D', \
	'lc-ms_label-free', 'lc-ms_labeled', 'lc-ms-ms_label-free', 'lc-ms-ms_labeled', \
	'LC-MS-untargeted', 'Lightsheet', 'MALDI-IMS', 'MIBI', 'NanoDESI', 'NanoPOTS', \
	'MxIF', 'PAS', 'bulk-RNA', 'SNARE-ATACseq2', 'SNARE-RNAseq2', 'scRNAseq-10xGenomics-v2', \
	'scRNAseq-10xGenomics-v3', 'sciATACseq', 'sciRNAseq', 'seqFish', \
	'snATACseq', 'snRNAseq-10xGenomics-v2', 'snRNAseq-10xGenomics-v3', 'Slide-seq', \
	'Targeted-Shotgun-LC-MS', 'TMT-LC-MS', 'WGS', 'LC-MS', 'MS', 'LC-MS_bottom_up', \
	'MS_bottom_up', 'LC-MS_top_down', 'MS_top_down']

table = []
headers = ['Status', 'Assay type', 'HuBMAP ID', 'Group name', 'Directory', 'Pickle file','Number of records','Number of files']
for assay in assays:
	print(assay)
	ids = hubmapbags.apis.get_hubmap_ids( assay, token=token )

	for id in ids:
		hubmap_id = id['hubmap_id']
		dataset = get_dataset_info( hubmap_id, instance='prod', token=token )
		
		if dataset is not None:
			if id['data_type'] == assay and id['status'] == 'Published':
				datum = [id['status'], \
					assay, \
					id['hubmap_id'], \
					id['group_name'], \
					generate_directory( dataset ), \
					generate_pickle_filename( dataset ), \
					get_number_of_records( generate_pickle_filename( dataset ) ), \
					compute_number_of_files( generate_directory( dataset ) ) ]
				table.append(datum)

print(tabulate(table, headers=headers,tablefmt="grid"))
df = pd.DataFrame (table, columns=headers)
df.to_csv( filename, index = False, sep="\t" )
df.to_pickle( filename.replace('tsv','pkl') )
