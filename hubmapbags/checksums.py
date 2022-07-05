import pandas as pd
from itertools import chain
from pathlib import Path
from shutil import rmtree
import datetime
import time
import os
import mimetypes
import urllib
import hashlib
import pickle

def __is_file_in_dataframe( df, filename ):
	return any(df['local_id'] == filename )

def __get_filename( file ):
	'''
	Helper method that returns a CFDE compatible version of a filename
	'''
	
	return file.name.replace(' ', '%20')

def __get_file_extension( file ):
	'''
	Helper method that returns the file extension. 
	'''
	return file.suffix

def __get_file_size( file ):
	'''
	Helper method that computes and returns the file size in bytes.
	'''

	return file.stat().st_size

def __get_md5( file ):
	'''
	Helper method that computes and return a file md5 checksum.
	'''

	blocksize=2**20
	m = hashlib.md5()

	with open( file, "rb" ) as f:
		while True:
			buf = f.read(blocksize)
			if not buf:
				break
			m.update( buf )

	return m.hexdigest()

def __get_relative_local_id( file, hubmap_uuid ):
	'''
	Helper function the return the relative local id.
	'''

	file = str(file)
	relative_local_id = file[file.find(hubmap_uuid)+len(hubmap_uuid)+1:]
	return relative_local_id

def __get_sha256( file ):
	'''
	Helper method that computes and return a file sha256 checksum.
	'''

	blocksize=2**20
	m = hashlib.md5()

	with open( file, "rb" ) as f:
		while True:
			buf = f.read(blocksize)
			if not buf:
				break
			m.update( buf )

	return m.hexdigest()

def __get_file_creation_date( file ):
	'''
	Helper method that return a file creation date.
	'''

	t = os.path.getmtime(str(file))
	return str(datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d'))

def __get_data_type( file ):
	'''
	Helper method that maps a file extension to an EDAM data format term.
	'''

	extension = __get_file_extension( file )

	try:
		formats = {}
		formats['.tsv'] = 'data:2526' #tsv 
		formats['.tif'] = 'data:2968' #tiff
		formats['.tiff'] = 'data:2968' #tiff
		formats['.png'] = 'data:2968' #png
		formats['.jpg'] = 'data:2968' #jpg
		formats['.ome.tiff'] = 'data:2968' #ome.tiff
		formats['.fastq'] = 'data:2044' #txt
		formats['.txt'] = 'data:2526' #txt
		formats['.xml'] = 'data:2526' #xml
		formats['.czi'] = 'data:2968' #czi
		formats['.gz'] = 'data:2044' #gz
		formats['.json'] = 'data:2526' #json
		formats['.xlsx'] = 'data:2526' #xlsx
		formats['._truncated_'] = '' #?
		formats['.tgz'] = '' #tgz
		formats['.tar.gz'] = '' #tar.gz
		formats['.csv'] = 'data:2526' #csv
		formats['.html'] = 'data:2526' #html
		formats['.htm'] = 'data:2526' #htm
		formats['.h5'] = '' #h5
		formats[''] = '' #other

		return formats[extension]
	except:
		print( 'Unable to find key for data type ' + extension )
		return ''

def __get_mime_type( file ):
	'''
	Helper function that return a file MIME type.
	'''
	
	return mimetypes.MimeTypes().guess_type(str(file))[0]

def __get_file_format( file ):
	'''
	Helper method that maps a file extension to an EDAM file format term.	
	'''

	extension = __get_file_extension( file )

	try:
		formats = {}
		formats['.tsv'] = 'format:2330' #tsv 
		formats['.tif'] = 'format:3547' #tiff
		formats['.tiff'] = 'format:3547' #tiff
		formats['.png'] = 'format:3547' #png
		formats['.jpg'] = 'format:3547' #jpg
		formats['.ome.tiff'] = 'format:3547' #ome.tiff
		formats['.fastq'] = 'format:2330' #txt
		formats['.txt'] = 'format:2330' #txt
		formats['.xml'] = 'format:2332' #xml
		formats['.czi'] = 'format:3547' #czi
		formats['.gz'] = 'format:3989' #gz
		formats['.json'] = 'format:2330' #json
		formats['.xlsx'] = 'format:3468' #xlsx
		formats['._truncated_'] = 'format:2330' #?
		formats['.tgz'] = 'format:3989' #tgz
		formats['.csv'] = 'format:3752' #csv
		formats['.html'] = 'format:2331' #html
		formats['.htm'] = 'format:2331' #htm
		formats['.tar.gz'] = 'format:3989' #tgz
		formats['.h5'] = 'format:3590' #h5
		formats[''] = ''

		return formats[extension]
	except:
		print('Unable to find key for file format ' + extension )
		return ''

def __get_dbgap_study_id( file, dbgap_study_id ):
	if dbgap_study_id == '':
		return ''
	else:
		if str(file).find('fastq.gz') > 0:
			return dbgap_study_id
		else:
			return ''

def __get_assay_type_from_obi(assay_type):
    assay = {}
    assay['af'] = 'OBI:0003087' #AF
    assay['atacseq-bulk'] = 'OBI:0003089' #Bulk ATAC-seq
    assay['bulk-rna'] = 'OBI:0001271' #Bulk RNA-seq
    assay['scrna-seq-10x'] = 'OBI:0002631' #scRNA-seq
    assay['snatacseq'] = 'OBI:0002762' #snATAC-seq
    assay['wgs'] = 'OBI:0002117' #WGS
    assay['codex'] = 'OBI:0003093' #CODEX    
    assay['lightsheet'] = 'OBI:0003098' #Lightsheet
    assay['imc'] = 'OBI:0001977' #IMC
    assay['imc3d'] = 'OBI:0001977' #IMC
    assay['maldi-ims-neg'] = 'OBI:0003099'
    assay['maldi-ims-pos'] = 'OBI:0003099'
    assay['pas'] = 'OBI:0003103'
    assay['slide-seq'] = 'OBI:0003107'
    assay['seqfish'] = 'OBI:0003094'
    assay['lc-ms-untargeted'] = 'OBI:0003097'
    assay['tmt-lc-ms'] = 'OBI:0003097'
    assay['targeted-shotgun-lc-ms'] = 'OBI:0003097'
    assay['snrnaseq']='OBI:0003109'
    assay['snare-atacseq2']='OBI:0003108'
    assay['snare-rnaseq2']='OBI:0003108'
    assay['snareseq']='OBI:0003108'
    assay['sciatacseq']='OBI:0003104'
    assay['cell-dive']='OBI:0001501'
    assay['scirnaseq'] = 'OBI:0002631'
    assay['scrnaseq-10xgenomics-v2'] = 'OBI:0002631'

    return assay[assay_type]

def _get_list_of_files( directory ):
    #p1 = Path(directory).glob('**//[!_drv]*')
    #p2 = Path(directory).glob('**//[!processed]*')
    #return chain(p1,p2)
    return Path(directory).glob('**/*')

def _compute( project_id, assay_type, directory, dbgap_study_id=None, dataset_hmid=None, dataset_uuid=None ):
	'''
	'''

	id_namespace = 'tag:hubmapconsortium.org,2022:'
	headers = ['id_namespace', \
		'local_id', \
		'project_id_namespace', \
		'project_local_id', \
		'persistent_id', \
		'creation_time', \
		'size_in_bytes', \
		'uncompressed_size_in_bytes', \
		'sha256', \
		'md5', \
		'compression_format', \
		'filename', \
		'file_format', \
		'data_type', \
		'assay_type', \
		'mime_type', \
		'bundle_collection_id_namespace', \
		'bundle_collection_local_id', \
		'dbgap_study_id']

    temp_file = directory.replace('/','_').replace(' ','_') + '.pkl'

	if Path(temp_file).exists():
		df = pickle.load(temp_file)
	else:
		df = pd.DataFrame(columns=headers)

	#ground truth
	p = _get_list_of_files( directory )
	print( 'Finding all files in directory' )

	counter = 0
	for file in p:
		if file.is_file():
				print('Processing ' + str(file) )
				if not __is_file_in_dataframe( df, str(file) ):
					df = df.append({'id_namespace':id_namespace, \
						'local_id':str(file).replace(' ','%20'), \
						'project_id_namespace':id_namespace, \
						'project_local_id':project_id, \
						'creation_time':__get_file_creation_date(file), \
						'size_in_bytes':__get_file_size(file), \
						'sha256':__get_sha256(file), \
						'filename':__get_filename(file), \
						'file_format':__get_file_format(file), \
						'compression_format':'', \
						'data_type':__get_data_type(file), \
						'assay_type':__get_assay_type_from_obi(assay_type), \
						'mime_type':__get_mime_type(file), \
						'bundle_collection_id_namespace':'', \
						'bundle_collection_local_id':'', \
						'dbgap_study_id':__get_dbgap_study_id(file,dbgap_study_id)}, ignore_index=True)
						counter = counter + 1

						if counter % 10000 == 0:
							print('Saving ' + str(counter) + 'to temporary file on disk.')
							with open( temp_file, 'wb' ) as file:
								pickle.dump( df, file )

	print('Saving final df to disk in file ' + temp_file)
	with open( temp_file, 'wb' ) as file:
		pickle.dump( df, file )

    return df