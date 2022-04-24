import sys
import pandas as pd
import os
import json
import time
import requests
import warnings
from . import utilities

def __get_instance( instance ):
	'''
	Helper method that returns the proper instance name
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
	Request ancestor info given a HuBMAP id.
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

def get_assay_types( instance='test', token=None, debug=False ):
	'''
	Request list of assay types from primary datasets.
	'''

	print('Get dataset information via the entity-api')
	r = __query_assay_types( instance='test', token=token, debug=debug )
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
		return j
