import csv
import json
from xlsxwriter.workbook import Workbook
import glob
import hubmapbags
import subprocess
from os.path import exists
from tabulate import tabulate 
import pandas as pd
from datetime import date
from os.path import basename

token='AgzPMdPlmEwwyQd50Bkw20Gy55O6vYynVkVbJQ6pJ105yBQd7JCkCylVn46Ev51mQW4p7nGwrawjVnIby3vKbf6nJm'
output_directory = 'reports'

today = date.today()
filename = 'file-report-' + str(today).replace('-','') + '.tsv'

def _get_number_of_files( directory, extension ):
    pathname = directory + "/**/*" + extension
    files = glob.glob(pathname, recursive=True)

    return len(files)

def get_file_frequency( directory ):
    extensions = ['.tsv', '.czi', '.xml', '.txt', '.tiff', '.gz', '.dat', '.tif', \
       '.xlsx', '.json', '.png', '.fcs', '.csv', '.pptx', '.gci', '.zip', \
       '.pdf', '.orig', '.scn', '.list', '.bam', '.raw', '.mzML']
    
    d = {}
    for key in extensions:
        d[key] =  _get_number_of_files( directory, key )
    
    return json.dumps(d)

def get_dataset_info( hubmap_id, instance='prod', token=None ):
	try:
		datasets = hubmapbags.magic.__extract_dataset_info_from_db( hubmap_id, token=token, instance=instance )
		for dataset in datasets.iterrows():
			dataset = dataset[1]
			return dataset
	except:
		return None

def get_registered_doi( dataset ):
	try:
		return dataset['registered_doi']
	except:
		return None

def get_doi_url( dataset ):
	try:
		return dataset['doi_url']
	except:
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
	
def human_readable_du( directory = None ):
        return subprocess.check_output(['du','-sh', directory]).split()[0].decode('utf-8')

def du( directory = None ):
	return subprocess.check_output(['du','-s', directory]).split()[0].decode('utf-8')

def find( directory = None ):
	return subprocess.check_output(['find',directory,'-type f | wc -l']).split()[0].decode('utf-8')

def compute_directory_size( directory = None ):
    # get size
    directory_size = 0
    for ele in os.scandir( directory ):
        directory_size+=os.path.getsize(ele)

    return convert_size(directory_size)

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s%s" % (s, size_name[i])

def compute_number_of_files( directory = None ):
    pathname = directory + "/**/*"
    files = glob.glob(pathname, recursive=True)

    return len(files)

def _get_number_of_jpgs( directory = None ):
    pathname = directory + "/**/*.jpg"
    files = glob.glob(pathname, recursive=True)

    return len(files)

def _get_number_of_pngs( directory = None ):
    pathname = directory + "/**/*.png"
    files = glob.glob(pathname, recursive=True)

    return len(files)

def _get_number_of_swcs( directory = None ):
    pathname = directory + "/**/*.swc"
    files = glob.glob(pathname, recursive=True)

    return len(files)

def _get_number_of_tifs( directory = None ):
    pathname = directory + "/**/*.tif"
    files = glob.glob(pathname, recursive=True)

    return len(files)

def _get_number_of_ome_tifs( directory = None ):
    pathname = directory + "/**/*.ome.tif"
    files = glob.glob(pathname, recursive=True)

    return len(files)

def _get_number_of_sequences( directory = None ):
    pathname = directory + "/**/*.fastq.gz"
    files = glob.glob(pathname, recursive=True)

    return len(files)

def _get_number_of_jp2s( directory = None ):
    pathname = directory + "/**/*.jp2"
    files = glob.glob(pathname, recursive=True)

    return len(files)

def _get_number_of_czis( directory = None ):
    pathname = directory + "/**/*.czi"
    files = glob.glob(pathname, recursive=True)

    return len(files)

assays = ['AF', 'ATACseq-bulk', 'cell-dive', 'CODEX', 'DART-FISH', 'IMC2D', 'IMC3D', \
	'lc-ms_label-free', 'lc-ms_labeled', 'lc-ms-ms_label-free', 'lc-ms-ms_labeled', \
	'LC-MS-untargeted', 'Lightsheet', 'MALDI-IMS', 'MIBI', 'NanoDESI', 'NanoPOTS', \
	'MxIF', 'PAS', 'bulk-RNA', 'SNARE-ATACseq2', 'SNARE-RNAseq2', 'scRNAseq-10xGenomics-v2', \
	'scRNAseq-10xGenomics-v3', 'sciATACseq', 'sciRNAseq', 'seqFish', \
	'snATACseq', 'snRNAseq-10xGenomics-v2', 'snRNAseq-10xGenomics-v3', 'Slide-seq', \
	'Targeted-Shotgun-LC-MS', 'TMT-LC-MS', 'WGS', 'LC-MS', 'MS', 'LC-MS_bottom_up', \
	'MS_bottom_up', 'LC-MS_top_down', 'MS_top_down']

table = []
headers = ['status', 'assay_type', 'hubmap_id', 'group_name', \
		'directory', 'registered_doi','doi_url', \
		'directory_size', 'human_directory_size', \
		'total_number_of_files', 'frequency']

for assay in assays:
	print(assay)
	ids = hubmapbags.apis.get_hubmap_ids( assay, token=token )

	for id in ids:
		hubmap_id = id['hubmap_id']
		dataset = get_dataset_info( hubmap_id, instance='prod', token=token )
		
		if dataset is not None:
			if id['data_type'] == assay and id['status'] == 'Published':
				directory = generate_directory(dataset)
				directory_size = du(directory)
				human_directory_size = human_readable_du(directory)
				total_number_of_files = compute_number_of_files(directory)
				frequency = get_file_frequency( directory )

				datum = [id['status'], \
					assay, \
					id['hubmap_id'], \
					id['group_name'], \
					directory, \
					get_registered_doi( dataset ), \
					get_doi_url( dataset ), \
					directory_size, \
					human_directory_size, \
					total_number_of_files, \
					frequency ]
				table.append(datum)

				df = pd.DataFrame(table, columns=headers)
				df.to_csv( filename, index = False, sep="\t" )
				df.to_pickle( filename.replace('tsv','pkl') )


print(tabulate(table, headers=headers,tablefmt="grid"))
# Add some command-line logic to read the file names.
tsv_file = filename
xlsx_file = filename.replace('tsv','xlsx')

# Create an XlsxWriter workbook object and add a worksheet.
workbook = Workbook(xlsx_file)
worksheet = workbook.add_worksheet()

# Create a TSV file reader.
tsv_reader = csv.reader(open(tsv_file, 'r'), delimiter='\t')

# Read the row data from the TSV file and write it to the XLSX file.
for row, data in enumerate(tsv_reader):
    worksheet.write_row(row, 0, data)

# Close the XLSX file.
workbook.close()
