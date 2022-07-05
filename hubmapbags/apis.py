import sys
import pandas as pd
import os
import json
from pathlib import Path
import time
import requests
from tabulate import tabulate
import warnings
from . import utilities
import glob

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
		return ''
	elif instance.lower() == 'test':
		return '.test'
	else:
		warnings.warn('Unknown option ' + str(instance) + '. Setting default value to test.')
		return '.test'

def __query_ancestors_info( hubmap_id, token=None, debug=False ):
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
		warnings.warn('Token not set.')
		return None

	URL='https://entity.api' + __get_instance( instance ) + '.hubmapconsortium.org/ancestors/'+hubmap_id
	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_ancestors_info( hubmap_id, instance='test', token=None, overwrite=True, debug=True ):
	'''
	Helper method that returns the ancestors info give a HuBMAP ID.
	'''

	directory = '.ancestors'
	file = os.path.join( directory, hubmap_id + '.json' )
	if os.path.exists( file ) and not overwrite:
		print('Loading existing JSON file')
		j = json.load( open( file, 'r' ) );
	else:
		print('Get information from ancestors via the entity-api ')
		r = __query_ancestors_info( hubmap_id, instance=instance, token=token, debug=debug )
		j = json.loads(r.itext)

	if j is None:
		warnings.warn('JSON object is empty.')
		return j
	elif 'message' in j:
		if debug:
			print('Request response. Not populating data frame and exiting script.')
		print(j['message'])
		return None
	else:
		if not os.path.exists( directory ):
			os.mkdir( directory )
		with open( file,'w') as outfile:
			json.dump(j, outfile, indent=4)
		return j

def __query_provenance_info( hubmap_id, instance='test', token=None, debug=False ):
	token =	utilities.__get_token( token )
	if token is None:
		warnings.warn('Token not set.')
		return None

	URL='https://entity.api' + __get_instance( instance ) + '.hubmapconsortium.org/datasets/'+hubmap_id+'/prov-info?format=json'
	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_provenance_info( hubmap_id, instance='test', token=None, overwrite=False, debug=False ):
	'''
	Request provenance info given a HuBMAP id.
	'''

	directory = '.provenance'
	file = os.path.join( directory, hubmap_id + '.json' )
	if os.path.exists( file ) and not overwrite:
		print('Loading existing JSON file')
		j = json.load( open( file, 'r' ) );
	else:
		print('Get information provenance info via the entity-api')
		r = __query_provenance_info( hubmap_id, instance=instance, token=token, debug=debug )
		j = json.loads(r.text)

	if j is None:
                warnings.warn('JSON object is empty.')
                return j
	elif 'message' in j:
		if debug:
			print('Request response. Not populating data frame and exiting script.')
		print(j['message'])
		return None
	else:
		if not os.path.exists( directory ):
			os.mkdir( directory )
		with open( file,'w') as outfile:
			json.dump(j, outfile, indent=4)
		return j

def __query_dataset_info( hubmap_id, instance='test', token=None, debug=False ):
	# gets the token to be able to access private data
	token = utilities.__get_token( token )
	if token is None:
		warnings.warn('Token not set.')
		return None

	URL='https://entity.api' + __get_instance( instance ) + '.hubmapconsortium.org/entities/' + hubmap_id
	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_dataset_info( hubmap_id, instance='test', token=None, overwrite=True, debug=True ):
	'''
	Request dataset info given a HuBMAP id.
	'''

	directory = '.datasets'
	file = os.path.join( directory, hubmap_id + '.json' )
	if os.path.exists( file ) and not overwrite:
		print('Loading existing JSON file')
		j = json.load( open( file, 'r' ) );
	else:
		print('Get dataset information via the entity-api')
		r = __query_dataset_info( hubmap_id, instance=instance, token=token, debug=debug )
		if r is None:
        	        warnings.warn('JSON object is empty.')
               		return r
		j = json.loads(r.text)

	if 'message' in j:
		if debug:
			print('Request response. Not populating data frame and exiting script.')
		print(j['message'])
		return None
	else:
		if not os.path.exists( directory ):
			os.mkdir( directory )
		with open( file,'w') as outfile:
			json.dump(j, outfile, indent=4)
		return j

def __query_assay_types( instance='test', token=None, debug=False ):
	token = utilities.__get_token( token )
	if token is None:
		warnings.warn('Token not set.')
		return None

	URL='https://search.api' + __get_instance( instance ) + '.hubmapconsortium.org/assaytype?primary=true&simple=true'
	headers={'Authorization':'Bearer '+token, 'accept':'application/json'}

	r = requests.get(URL, headers=headers)
	return r

def get_assay_types( debug=False ):
	'''
	Request list of assay types from primary datasets.
	'''

	if debug:
		print('Get dataset information via the entity-api')
	r = __query_assay_types( debug=debug )
	if r is None:
		warnings.warn('JSON object is empty.')
		return r
	j = json.loads(r.text)

	if 'message' in j:
		if debug:
			warnings.warn('Request response empty.')
		print(j['message'])
		return None
	else:
		return j['result']

def __query_assay_types( debug=False ):
	'''
	Search dataset by a given assaytype name.
	'''

	url = 'https://search.api.hubmapconsortium.org/assaytype'

	headers = {'Accept': 'application/json'}
	params = {'primary':'true', 'simple':'true'}

	data = requests.get(url=url, headers=headers, params=params)
	return data

def get_hubmap_ids( assay_name, token=None, debug=False ):
	'''
	Get list of HuBMAP ids given an assay name.
	'''

	token = utilities.__get_token( token )
	if token is None:
		warnings.warn('Token not set.')
		return None

	answer =  __query_hubmap_ids( assay_name, token=token, debug=debug )
	data = answer['hits']['hits']

	results = []
	for datum in data:
		results.append(	{
			'uuid':datum['_source']['uuid'], \
			'hubmap_id':datum['_source']['hubmap_id'], \
			'status':datum['_source']['status'], \
			'data_type':datum['_source']['data_types'][0], \
			'group_name':datum['_source']['group_name'] })

	return results

def __query_hubmap_ids( assayname, token=None, debug=False ):
	'''
	Search dataset by a given assaytype name.
	'''

	url = 'https://search.api.hubmapconsortium.org/search'

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
