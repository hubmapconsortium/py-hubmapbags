import sys
import pandas as pd
import os
from os.path import exists
import json
import time
import requests
import warnings
from . import utilities
from . import magic
from . import apis

def populate_local_file_with_remote_uuids( hubmap_id, instance='test', token=None, overwrite=False, debug=False ):

	utilities.pprint('Processing dataset with HuBMAP ID ' + hubmap_id)
	dataset = magic.__extract_dataset_info_from_db( hubmap_id, token=token, instance=instance )

	if dataset is None:
		warnings.warn('No datasets found. Exiting.')
		return False

	dataset = dataset.squeeze()
	data_directory = dataset['full_path']
	computing = data_directory.replace('/','_').replace(' ','_') + '.computing'
	done = '.' + data_directory.replace('/','_').replace(' ','_') + '.done'
	broken = '.' + data_directory.replace('/','_').replace(' ','_') + '.broken'
	temp_file = data_directory.replace('/','_').replace(' ','_') + '.pkl'

	if exists( computing ):
		warnings.warn('Computing file ' + computing + ' exists. Another process is computing checksums. Not populating local file.')
		return False
	elif not exists( computing ) and not exists( done ):
		print('File ' + done + ' not found on disk. Not populating local file.' )
		return False
	elif exists( done ):
		if exists( temp_file ):
			if should_i_generate_uuids( hubmap_id, temp_file, instance=instance, token=token, debug=debug ):
				print('Attempting to populate local file')
				df = pd.read_pickle( temp_file )
				uuids = get_uuids( hubmap_id, instance=instance, token=token, debug=debug )

				for i in range(len(df)):
					for uuid in uuids:
						if df.loc[i,'relative_local_id'] == uuid['path']:
							df.loc[i,'hubmap_uuid'] = \
								uuid['file_uuid']

				print('Saving temp file ' + temp_file + ' to disk.')
				df.to_pickle( temp_file )
				return True
			else:
				return False

def __get_instance( instance ):
	'''
	Helper method that determines what instance to use.
	'''

	if instance.lower() == 'dev':
		return '.dev'
	elif instance.lower() == 'prod':
		return 'prod'
	elif instance.lower() == 'test':
		return '.test'
	else:
		if instance is None:
			warnings.warn('Instance not set. Setting default value to "test".')
		else:
			warnings.warn('Unknown option ' + str(instance) + '. Setting default value to test.')
		return '.test'

def __query_existence( uuid, instance='prod', token=None, debug=False ):
	token = utilities.__get_token( token )
	if token is None:
		warnings.warn('Token not set.')
		return None

	if __get_instance( instance ) == 'prod':
        	URL='https://uuid.api.hubmapconsortium.org/'+uuid+'/exists'
	else:
        	URL='https://uuid-api' + __get_instance( instance ) + '.hubmapconsortium.org/'+uuid+'/exists'

	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def __query_uuids( hubmap_id, instance='prod', token=None, debug=False ):
	token = utilities.__get_token( token )
	if token is None:
		warnings.warn('Token not set.')
		return None

	if __get_instance( instance ) == 'prod':
		URL = 'https://uuid.api.hubmapconsortium.org/'+hubmap_id+'/files'
	else:
	        URL='https://uuid-api' + __get_instance( instance ) + '.hubmapconsortium.org/'+hubmap_id+'/files'

	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_uuids( hubmap_id, instance='prod', token=None, debug=False ):
	'''
	Get UUIDs, if any, given a HuBMAP id.
	'''

	r = __query_uuids( hubmap_id, instance=instance, token=token, debug=debug )
	j = json.loads(r.text)

	return j

def get_number_of_uuids( hubmap_id, instance='prod', token=None, debug=False ):
	'''
	Get number of UUIDs associated with this HuBMAP id using the UUID API.
	'''

	try:
		return len(get_uuids( hubmap_id, instance=instance, token=token, debug=debug ))
	except:
		return 0

def has_uuids( hubmap_id, instance='prod', token=None ):
	if get_number_of_uuids( hubmap_id, instance='prod', token=token ) == 0:
		return False
	else:
		return True

def generate( file, instance='prod', debug=True ):
	'''
	Main function that generates UUIDs using the uuid-api.
	'''

	if debug:
		print('Processing ' + file + '.')

	# icaoberg since neither the hubmap id nor the uuid are save in the dataframe
	# extract it from the filename
	duuid=file.split('_')[-1].split('.')[0]

	token = utilities.__get_token()
	if token is None:
		warnings.warn('Token not set.')
		return None

	try:
		if debug:
			print('Loading temp file ' + file + '.')
		df = pd.read_pickle( file )
	except:
		if debug:
			print('Unable to load pickle file ' + file + '. Exiting script.' )
		return False

	if __get_instance( instance ) == 'prod':
		URL = 'https://uuid.api.hubmapconsortium.org/hmuuid/'
	else:
		URL = 'https://uuid-api' + __get_instance( instance ) + '.hubmapconsortium.org/hmuuid/'

	headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0','Authorization':'Bearer '+token, 'Content-Type':'application/json'}

	if len(df) <= 1000:
		if df['hubmap_uuid'].isnull().all():
			file_info = []
			for datum in df.iterrows():
				datum = datum[1]
				filename = datum['local_id'][datum['local_id'].find(duuid)+len(duuid)+1:]
				file_info.append({'path':filename, \
					'size':datum['size_in_bytes'], \
					'checksum':datum['sha256'], \
				'base_dir':'DATA_UPLOAD'})

			payload = {}
			payload['parent_ids']=[duuid]
			payload['entity_type']='FILE'
			payload['file_info']=file_info
			params = {'entity_count':len(file_info)}

			if debug:
				print('Generating UUIDs')
			r = requests.post(URL, params=params, headers=headers, data=json.dumps(payload), allow_redirects=True, timeout=120)
			j = json.loads(r.text)

			if 'message' in j:
				if debug:
					print('Request response. Not populating data frame and exiting script.')
				print(j['message'])
				return False
			else:
				for datum in j:
					df.loc[df['local_id'].str.contains(datum['file_path']),'hubmap_uuid']=datum['uuid']
  
				if debug:
					print('Updating pickle file ' + file + ' with the request response.')

				df.to_pickle(file)
				with open(file.replace('pkl','json'),'w') as outfile:
					json.dump(j, outfile, indent=4)
		else:
			if debug:
				print('HuBMAP uuid column is populated. Skipping generation.')
	else:
		if debug:
			print('Data frame has ' + str(len(df)) + ' items. Partitioning into smaller chunks.')

		n = 1000  #chunk row size
		dfs = [df[i:i+n] for i in range(0,df.shape[0],n)]

		counter = 0
		for frame in dfs:
			counter=counter+1
			if debug:
				print('Computing uuids on partition ' + str(counter) + ' of ' + str(len(dfs)) + '.')

			file_info = []
			for datum in frame.iterrows():
				datum = datum[1]
				filename = datum['local_id'][datum['local_id'].find(duuid)+len(duuid)+1:]
				file_info.append({'path':filename, \
					'size':datum['size_in_bytes'], \
					'checksum':datum['sha256'], \
				'base_dir':'DATA_UPLOAD'})

			payload = {}
			payload['parent_ids']=[duuid]
			payload['entity_type']='FILE'
			payload['file_info']=file_info
			params = {'entity_count':len(file_info)}

			if frame['hubmap_uuid'].isnull().all():
				if debug:
					print('Generating uuids')

				r = requests.post(URL, params=params, headers=headers, data=json.dumps(payload), allow_redirects=True, timeout=120)
				j = json.loads(r.text)
				time.sleep(5)

				if 'message' in j:
					if debug:
						print('Request response. Not populating data frame.')
					print(j['message'])
					return False
				else:
					for datum in j:
						df.loc[df['local_id'].str.contains(datum['file_path']),'hubmap_uuid']=datum['uuid']

					if debug:
						print('Updating pickle file ' + file + ' with the results of this chunk.')
					df.to_pickle(file)
			else:
				if debug:
					print('HuBMAP uuid chunk is populated. Skipping recomputation.')

def should_i_generate_uuids( hubmap_id, instance='prod', token=None, debug=False ):
	'''
	Helper function that compares the number of files on disk versus the number of
	entries in the UUID-API database.
	'''

	number_of_files = apis.get_number_of_files( hubmap_id, instance=instance, token=token )
	number_of_entries_in_db = get_number_of_uuids( hubmap_id, instance=instance, token=token )

	if number_of_entries_in_db == 0:
		return True

	if number_of_entries_in_db == number_of_files:
		return False

	if number_of_files != 0 and number_of_files > number_of_entries_in_db:
		warning('There are more entries in local file than in database. Either a job is running computing checksums or a job failed.')
		return None

	if number_of_files != 0 and number_of_files < number_of_entries_in_db:
		warning('There are more entries in database than files in local db. More than likely UUIDs were generate more than once. Contact a system administrator.')
		return None

def is_complete( hubmap_id, instance='prod', token=None, debug=False ):
	number_of_files = apis.get_number_of_files( hubmap_id, instance=instance, token=token )
	number_of_entries_in_db = get_number_of_uuids( hubmap_id, instance=instance, token=token )

	return number_of_files == number_of_entries_in_db
