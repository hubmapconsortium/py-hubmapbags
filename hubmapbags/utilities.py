import sys
import os
import pandas as pd
from tabulate import tabulate

def __get_token( token=None ):
	'''
	Helper method that gets the HuBMAP services token.
	'''

	if token is None:
		token = os.getenv('TOKEN')

	return token

def add_duuid_column( file ):
	'''
	Helper method that adds the UUID column of a backup pickle file.

	:param file: A pickle file 
	:type file: string
	:rtype: boolean
	'''

	if file.find('.pkl') > 0:
		try:
			duuid=file.split('_')[-1].split('.')[0]
			print(file)

			df = pd.read_pickle( file )
			df['duuid']=duuid
			df.to_pickle( file )
			return True
		except:
			return False

	if file.find('.tsv') > 0:
		try:
			duuid=file.split('_')[-1].split('.')[0]
			print(file)

			df = pd.read_csv( file, sep='\t' )
			df['duuid']=duuid
			df.to_csv( file, sep='\t', index=False )
			return True
		except:
			return False

	return False

def reset_hubmap_uuid_column( file ):
	'''
	Helper method that resets the UUID column of a backup pickle file.

	:param file: A pickle file 
	:type file: string
	:rtype: boolean
	'''

	try:
		df = pd.read_pickle( file )
		df['hubmap_uuid']=None
		df.to_pickle( file )
		return True
	except:
		return False

def add_empty_dbgap_study_id_column( file ):
	'''
	Helper function that adds a dbGaP study ID column to a backup pickle file.

	:param file: A pickle file 
	:type file: string
	:rtype: boolean
	'''

	duuid=file.split('_')[-1].split('.')[0]
	print(file)

	df = pd.read_pickle( file )
	df['dbgap_study_id']=None
	df.to_pickle(file)

def pprint( message ):
	'''
	Helper method that pretty prints a string.

	:param file: A message 
	:type file: string
	'''

	table = [[message]]
	output = tabulate(table, tablefmt='grid')
	print(output)

def generate_uuid_report( directory, debug=False ):
	p = Path(directory).glob('_hive*.tsv')

	headers = ['did','duuid','filename']
	df = pd.DataFrame(columns = headers)

	for file in p:
		file = str(file)

		if debug:
			print(file)

		instance = 'prod'

		temp = pd.read_csv( file, sep='\t' )
		row = pd.DataFrame([{ \
			'did':temp.loc[0]['did'], \
			'duuid':temp.loc[0]['duuid'], \
			'filename':file, \
			'number_of_records':int(len(temp)), \
			'number_of_remote_uuids':hubmapbags.uuids.get_number_of_uuids( temp.loc[0]['did'], instance=instance, debug=False ), \
			'uuid-api-instance':instance}])
		df = pd.concat([df,row], axis=0, ignore_index=True)

