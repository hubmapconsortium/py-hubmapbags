from logging import warning
import sys
import pandas as pd
import os
import json
from pathlib import Path
import time
import requests
from tabulate import tabulate
from warnings import warn as warning
from . import utilities
import glob

def is_primary( hubmap_id, instance='prod', token=None ):
	'''
	Returns true if the dataset is a primary dataset.
	'''

	metadata = get_ancestors_info( hubmap_id, instance=instance, token=token )
	if 'entity_type' in metadata[0].keys() and  metadata[0]['entity_type'] == 'Sample':
		return True
	else:
		if 'error' in metadata[0]:
			warning(metadata[0]['error'])
		return False

def __compute_number_of_files( directory = None ):
	'''
	Helper function that returns the number of files in a loca directory.
	'''
	pathname = directory + "/**/*"
	files = glob.glob(pathname, recursive=True)

	return len(files)

def __check_if_folder_is_empty( directory = None ):
	'''
	If the local directory is empty, then it returns true. False, otherwise.
	'''

	if not os.listdir( directory ):
		return True
	else:
		return False

def __get_instance( instance ):
	'''
	Helper method that returns the proper instance name.
	'''

	if instance.lower() == 'dev':
		return '.dev'
	elif instance.lower() == 'prod':
		return 'prod'
	elif instance.lower() == 'test':
		return '.test'
	else:
		warning('Unknown option ' + str(instance) + '. Setting default value to test.')
		return '.test'

def __query_ancestors_info( hubmap_id, instance='prod', token=None, debug=False ):
	'''
	Helper method that returns the ancestors info give a HuBMAP ID.

	:param hubmap_id: valid HuBMAP ID
	:type hubmap_id: string
	:param token: a valid token
	:type token: None or string
	:param debug: debug flag
	:type debug: boolean
	:rtype: request response
	'''

	token =	utilities.__get_token( token )
	if token is None:
		warning('Token not set.')
		return None

	if __get_instance( instance ) == 'prod':
		URL='https://entity.api.hubmapconsortium.org/ancestors/'+hubmap_id
	else:
		URL='https://entity-api' + __get_instance( instance ) + '.hubmapconsortium.org/ancestors/'+hubmap_id

	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_ancestors_info( hubmap_id, instance='prod', token=None, overwrite=True, debug=False ):
	'''
	Helper method that returns the ancestors info give a HuBMAP ID.
	'''

	directory = '.ancestors'
	file = os.path.join( directory, hubmap_id + '.json' )
	if os.path.exists( file ) and not overwrite:
		if debug:
			print('Loading existing JSON file.')
		j = json.load( open( file, 'r' ) );
	else:
		if debug:
			print('Get information from ancestors via the entity-api.')
		r = __query_ancestors_info( hubmap_id, instance=instance, token=token, debug=debug )
		j = json.loads(r.text)

	if j is None:
		warning('JSON object is empty.')
		return j
	elif 'message' in j:
		warning('Request response is empty. Not populating dataframe.')
		print(j['message'])
		return None
	else:
		if not os.path.exists( directory ):
			os.mkdir( directory )
		with open( file,'w') as outfile:
			json.dump(j, outfile, indent=4)
		return j

def __query_provenance_info( hubmap_id, instance='prod', token=None, debug=False ):
	token =	utilities.__get_token( token )
	if token is None:
		warning('Token not set.')
		return None

	if __get_instance( instance ) == 'prod':
		URL='https://entity.api.hubmapconsortium.org/datasets/'+hubmap_id+'/prov-info?format=json'
	else:
		URL='https://entity-api' + __get_instance( instance ) + '.hubmapconsortium.org/datasets/'+hubmap_id+'/prov-info?format=json'

	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}
	r = requests.get(URL, headers=headers)
	return r

def __query_dataset_info( hubmap_id, instance='prod', token=None, debug=False ):
	token = utilities.__get_token( token )
	if token is None:
		warning('Token not set.')
		return None

	if __get_instance( instance ) == 'prod':
		URL='https://entity.api.hubmapconsortium.org/entities/' + hubmap_id
	else:
		URL='https://entity-api' + __get_instance( instance ) + '.hubmapconsortium.org/entities/' + hubmap_id

	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_dataset_info( hubmap_id, instance='prod', token=None, overwrite=True, debug=True ):
	'''
	Request dataset info given a HuBMAP id.
	'''

	directory = '.datasets'
	file = os.path.join( directory, hubmap_id + '.json' )
	if os.path.exists( file ) and not overwrite:
		j = json.load( open( file, 'r' ) );
	else:
		r = __query_dataset_info( hubmap_id, instance=instance, token=token, debug=debug )
		if r is None:
			warning('JSON object is empty.')
			return r
		j = json.loads(r.text)

	if 'message' in j:
		warning('Request response is empty. Not populating dataframe.')
		print(j['message'])
		return None
	else:
		if not os.path.exists( directory ):
			os.mkdir( directory )
		with open( file,'w') as outfile:
			json.dump(j, outfile, indent=4)
		return j

def __query_assay_types( instance='prod', token=None, debug=False ):
	token = utilities.__get_token( token )
	if token is None:
		warning('Token not set.')
		return None

	if __get_instance( instance ) == 'prod':
		URL='https://search.api.hubmapconsortium.org/v3/assaytype?primary=true&simple=true'
	else:
		URL='https://search-api' + __get_instance( instance ) + '.hubmapconsortium.org/v3/assaytype?primary=true&simple=true'

	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_provenance_info( hubmap_id, instance='prod', token=None, overwrite=False, debug=False ):
	'''
	Request provenance info given a HuBMAP id.
	'''

	directory = '.provenance'
	file = os.path.join( directory, hubmap_id + '.json' )
	if os.path.exists( file ) and not overwrite:
		if debug:
			print('Loading existing JSON file')
		j = json.load( open( file, 'r' ) );
	else:
		if debug:
			print('Get information provenance info via the entity-api.')
		r = __query_provenance_info( hubmap_id, instance=instance, token=token, debug=debug )
		j = json.loads(r.text)

	if j is None:
		warning('JSON object is empty.')
		return j
	elif 'message' in j:
		warning('Request response is empty. Not populating dataframe.')
		print(j['message'])
		return None
	else:
		if not os.path.exists( directory ):
			os.mkdir( directory )
		with open( file,'w') as outfile:
			json.dump(j, outfile, indent=4)
		return j

def get_hubmap_ids( assay_name, token=None, debug=False ):
	'''
	Get list of HuBMAP ids given an assay name.
	'''

	token = utilities.__get_token( token )
	if token is None:
		warning('Token not set.')
		return None

	answer =  __query_hubmap_ids( assay_name, token=token, debug=debug )

	if 'error' in answer.keys():
		warning(answer['error'])
		return None

	data = answer['hits']['hits']

	results = []
	for datum in data:
		results.append(	{
			'uuid':datum['_source']['uuid'], \
			'hubmap_id':datum['_source']['hubmap_id'], \
			'status':datum['_source']['status'], \
			'is_protected':is_protected( datum['_source']['hubmap_id'], instance='prod', token=token ), \
			'is_primary':is_primary(datum['_source']['hubmap_id'], instance='prod', token=token), \
			'data_type':datum['_source']['data_types'][0], \
			'group_name':datum['_source']['group_name'] })

	return results

def __query_hubmap_ids( assayname, token=None, debug=False ):
	'''
	Search dataset by a given assaytype name.
	'''

	url = 'https://search.api.hubmapconsortium.org/v3/search'

	if token is None:
		headers = {'Accept': 'application/json'}
	else:
		headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	body = {
		"size": 500,
		"_source": {
			"include": ["hubmap_id","uuid","group_name","status","data_types"]},
		"query": {
			"bool": {
				"must": [{"match_phrase":{"data_types": assayname}}],
	"filter": [{"match":{"entity_type": "Dataset"}}]
		}
		}
	}

	data = requests.post(url=url, headers=headers, json=body).json()
	return data

def __is_valid( file ):
	string1 = 'No error'
	file1 = open( file, "r")
	readfile = file1.read()

	answer = 'INVALID'
	if string1 in readfile:
		answer='VALID'

	file1.close()
	return answer

def is_protected( hubmap_id, instance='prod', token=None ):
	token = utilities.__get_token( token )
	if token is None:
		warning('Token not set.')

	metadata = get_dataset_info( hubmap_id, instance=instance, token=token )

	if 'contains_human_genetic_sequences' in metadata.keys():
		return metadata['contains_human_genetic_sequences']
	else:
		return None

def pretty_print_info_about_new_datasets( assay_name, debug=False ):
	'''
	Pretty print results about datasets in 'New' status.
	'''

	if debug:
		print( 'Using search API to retrieve datasets with data type ' + assay_name )
	answer = get_hubmap_ids( assay_name, debug=False )

	data = [["UUID","HuBMAP ID", "Directory", "Status", "Number of files", "Metadata TSV", "Contributors TSV", "Antibodies TSV", "Validation status"]]
	for datum in answer:
		if debug:
			print( 'Processing dataset ' + datum['uuid'] )
		if datum['status'] == 'New':
			if Path( '/hive/hubmap/data/consortium/' + datum['group_name'] + '/' + datum['uuid'] ).exists():
				directory = '/hive/hubmap/data/consortium/' + datum['group_name'] + '/' + datum['uuid'] + '/'
				number_of_files = __compute_number_of_files( directory )
				metadata = glob.glob( directory + '*metadata*.tsv' )
				if not metadata:
					metadata = ''
				else:
					metadata = metadata[0].replace( directory, '' )

				contributors = glob.glob( directory + '*contributors*.tsv' )
				if not contributors:
					contributors = ''
				else:
					contributors = contributors[0].replace( directory, '' )

				antibodies = glob.glob( directory + '/*antibodies*.tsv' )
				if not antibodies:
					antibodies = ''
				else:
					antibodies = antibodies[0].replace( directory, '' )

				report = glob.glob( directory + 'validation_report.txt' )
				if not report:
					report = '-'
				else:
					report = __is_valid( directory + 'validation_report.txt' )
			else:
				directory = 'NOT AVAILABLE'
				number_of_files = '-'
				metadata = ''
				contributors = ''
				antibodies = ''

			data.append([datum['uuid'],datum['hubmap_id'],directory,datum['status'],number_of_files, metadata, contributors, antibodies, report])

	table = tabulate(data,headers='firstrow',tablefmt='grid')
	utilities.pprint( assay_name )
	print(table)

def pretty_print_info_about_all_new_datasets( filename=None, debug=False ):
	'''
	Pretty print results about datasets in 'New' status.
	'''

	if debug:
		utilities.pprint('Retrieving list of assay types')
	assay_types = get_assay_types()

	answer = []
	for assay_type in assay_types:
		if debug:
			print( 'Using search API to retrieve datasets with data type ' + assay_type )
		answer.extend( get_hubmap_ids( assay_type, debug=debug ) )

	data = [["UUID","HuBMAP ID", "Assay Type", "Status", "Directory", "Is empty?", "Metadata TSV", "Contributors TSV", "Antibodies TSV", "Validation status"]]
	if debug:
		utilities.pprint( 'Processings datasets' )

	for datum in answer:
		if debug:
			print( 'Processing dataset ' + datum['uuid'] )
		if datum['status'] == 'New':
			if Path( '/hive/hubmap/data/consortium/' + datum['group_name'] + '/' + datum['uuid'] ).exists():
				directory = '/hive/hubmap/data/consortium/' + datum['group_name'] + '/' + datum['uuid'] + '/'
				metadata = glob.glob( directory + '*metadata*.tsv' )
				if not metadata:
					metadata = ''
				else:
					metadata = metadata[0].replace( directory, '' )

				contributors = glob.glob( directory + '*contributors*.tsv' )
				if not contributors:
					contributors = ''
				else:
					contributors = contributors[0].replace( directory, '' )

				antibodies = glob.glob( directory + '/*antibodies*.tsv' )
				if not antibodies:
					antibodies = ''
				else:
					antibodies = antibodies[0].replace( directory, '' )

				report = glob.glob( directory + 'validation_report.txt' )
				if not report:
					report = '-'
				else:
					report = __is_valid( directory + 'validation_report.txt' )

				is_empty = __check_if_folder_is_empty( directory )
			else:
				directory = 'NOT AVAILABLE'
				is_empty = ''
				number_of_files = '-'
				metadata = ''
				contributors = ''
				antibodies = ''

			data.append([datum['uuid'], datum['hubmap_id'], \
				datum['data_type'],datum['status'], directory, \
				is_empty, metadata, contributors, \
				antibodies, report])

	if filename:
		df = pd.DataFrame( data[1:], columns=["UUID","HuBMAP ID", "Assay Type", "Status", "Directory", "Is empty?", "Metadata TSV", "Contributors TSV", "Antibodies TSV", "Validation status"] )
		df.to_csv( filename, sep='\t', index=False )

	table = tabulate(data,headers='firstrow',tablefmt='grid')
	print(table)

def pretty_print_hubmap_ids( assay_name, debug=False ):
	'''
	Pretty print results.
	'''

	answer = get_hubmap_ids( assay_name, debug=False )

	data = [["UUID","HuBMAP ID","Group Name","Status"]]
	for datum in answer:
		data.append([datum['uuid'],datum['hubmap_id'],datum['group_name'], datum['status']])

	table = tabulate(data,headers='firstrow',tablefmt='grid')
	print(table)

def get_directory( hubmap_id, instance='prod', token=None ):
	'''
	Returns the local directory given a valid HuBMAP dataset ID.
	'''

	metadata = get_dataset_info( hubmap_id, instance=instance, token=token )
	if 'error' in metadata:
		warning(metadata['error'])
		return None

	if 'contains_human_genetic_sequences' in metadata and metadata['contains_human_genetic_sequences']:
		directory = '/hive/hubmap/data/protected/' + metadata['group_name'] + '/' + metadata['uuid']
	else:
		directory = '/hive/hubmap/data/public/' + metadata['uuid']

	return directory

def get_files( hubmap_id, instance='prod', token=None ):
	'''
	Returns the list of files in the file system corresponding to the HuBMAP ID.
	'''

	directory = get_directory( hubmap_id, instance=instance, token=token )

	try:
		if Path(directory).exists():
			files = list(Path(directory).glob('**/*'))
			files = [x for x in files if x.is_file() ]
			return files
		else:
			return None
	except:
		warning('Unable to access files in directory. More than likely a permission file.')
		return None

def get_number_of_files( hubmap_id, instance='prod', token=None ):
	'''
	'''

	answer = get_files( hubmap_id, instance=instance, token=token )

	if answer is None:
		return None
	else:
		return len(answer)

def __query_donor_info( hubmap_id, instance='prod', token=None, debug=False ):
	token = utilities.__get_token( token )
	if token is None:
		warning('Token not set.')
		return None

	if __get_instance( instance ) == 'prod':
		URL='https://entity.api.hubmapconsortium.org/entities/' + hubmap_id
	else:
		URL='https://entity-api' + __get_instance( instance ) + '.hubmapconsortium.org/entities/' + hubmap_id

	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_donor_info( hubmap_id, instance='prod', token=None, overwrite=True, debug=True ):
	'''
	Request dataset info given a HuBMAP id.
	'''

	# get donor ID from dataset ID
	metadata = get_provenance_info( hubmap_id, instance=instance, token=token, debug=debug )
	hubmap_donor_id = metadata['donor_hubmap_id'][0]

	directory = '.donor'
	file = os.path.join( directory, hubmap_donor_id + '.json' )
	if os.path.exists( file ) and not overwrite:
		j = json.load( open( file, 'r' ) );
	else:
		r = __query_donor_info( hubmap_donor_id, instance=instance, token=token, debug=debug )
		if r is None:
			warning('JSON object is empty.')
			return r
		j = json.loads(r.text)

	if 'message' in j:
		warning('Request response is empty. Not populating dataframe.')
		print(j['message'])
		return None
	else:
		if not os.path.exists( directory ):
			os.mkdir( directory )
		with open( file,'w') as outfile:
			json.dump(j, outfile, indent=4)
		return j

def __query_entity_info( hubmap_id, instance='prod', token=None, debug=False ):
	token = utilities.__get_token( token )
	if token is None:
		warning('Token not set.')
		return None

	if __get_instance( instance ) == 'prod':
		URL='https://entity.api.hubmapconsortium.org/entities/' + hubmap_id
	else:
		URL='https://entity-api' + __get_instance( instance ) + '.hubmapconsortium.org/entities/' + hubmap_id

	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_entity_info( hubmap_id, instance='prod', token=None, overwrite=True, debug=True ):
	'''
	Request dataset info given a HuBMAP id.
	'''

	directory = '.entity'
	file = os.path.join( directory, hubmap_id + '.json' )
	if os.path.exists( file ) and not overwrite:
		j = json.load( open( file, 'r' ) );
	else:
		r = __query_entity_info( hubmap_id, instance=instance, token=token, debug=debug )
		if r is None:
			warning('JSON object is empty.')
			return r
		j = json.loads(r.text)

	if 'message' in j:
		warning('Request response is empty. Not populating dataframe.')
		print(j['message'])
		return None
	else:
		if not os.path.exists( directory ):
			os.mkdir( directory )
		with open( file,'w') as outfile:
			json.dump(j, outfile, indent=4)
		return j

def get_assay_types( token=None, debug=False ):
	'''
	Request list of assay types.
	'''

	if debug:
		print('Get dataset information via the search-api.')
	assays = __query_assay_types( token=token, debug=debug )

	return assays

def __query_assay_types( token=None, debug=False ):
	'''
	Search dataset by a given assaytype name.
	'''

	token = utilities.__get_token( token )
	if token is None:
		warning('Token not set.')
		return None

	url = 'https://search.api.hubmapconsortium.org/v3/search'

	headers = {'Authorization':'Bearer '+token, 'accept':'application/json'}
	body = {
    "query": {
        "bool": {
            "must": [
                {
                    "match_phrase": {
                        "entity_type": "dataset"
                    }
                }
            ]
        }
    },
    "aggs": {
        "fieldvals": {
            "terms": {
                "field": "data_types.keyword",
                "size": 500
            	}
        	}
    	}
	}

	data = requests.post(url=url, headers=headers, json=body).json()
	data = data['aggregations']['fieldvals']['buckets']

	assays = []
	for datum in data:
		assays.append(datum['key'])

	return sorted(assays)