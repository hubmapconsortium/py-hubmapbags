import hubmapbags
from datetime import datetime
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from warnings import warn as warning
from datetime import datetime

token = ''
instance = 'prod' #default instance is test

now = datetime.now()
output_directory = 'data'
if not Path(output_directory).exists():
	Path(output_directory).mkdir()

report_output_directory = 'uuid-protected-data-report'
if not Path(report_output_directory).exists():
    Path(report_output_directory).mkdir()
report_output_filename = report_output_directory + '/' + str(now.strftime('%Y%m%d')) + '.tsv'

if not Path(report_output_filename).exists():
	# get assay types
	assay_names = hubmapbags.get_assay_types()

	report = pd.DataFrame()
	for assay_name in assay_names:
		print(assay_name)
		datasets = pd.DataFrame(hubmapbags.get_hubmap_ids( assay_name=assay_name, token=token ))

		if datasets.empty:
			continue

		#clean up
		datasets = datasets[(datasets['data_type'] != 'image_pyramid')]
		datasets = datasets[(datasets['status'] == 'Published')]
		datasets = datasets[(datasets['is_protected'] == True)]

		datasets['has_uuids'] = None
		datasets['number_of_uuids'] = None
		datasets['directory'] = None
		datasets['number_of_files'] = None

		for index, datum in tqdm(datasets.iterrows()):
			if hubmapbags.apis.is_protected( datum['hubmap_id'], token=token ):
				datasets.loc[index, 'number_of_uuids'] = hubmapbags.uuids.get_number_of_uuids( datum['hubmap_id'], instance='prod', token=token )

				if datasets.loc[index, 'number_of_uuids'] == 0:
					datasets.loc[index, 'has_uuids'] = False
				else:
					datasets.loc[index, 'has_uuids'] = True

				datasets.loc[index, 'directory'] = hubmapbags.apis.get_directory( datum['hubmap_id'], instance='prod', token=token )
				datasets.loc[index, 'number_of_files'] = hubmapbags.apis.get_number_of_files( datum['hubmap_id'], instance='prod', token=token )

		if report.empty:
			report = datasets
		else:
			report = pd.concat( [report, datasets] )

		report = report[report['is_protected']==True] 
		report.to_csv( report_output_filename, sep='\t', index=False )
		report.to_pickle( report_output_filename.replace('tsv','pkl') )
else:
	print('File found on disk. Loading ' + report_output_filename + '.' )
	report = pd.read_pickle( report_output_filename.replace('tsv', 'pkl') )

def get_dbgap_study_id( datum ):
	Index(['uuid', 'hubmap_id', 'status', 'is_protected', 'data_type',
       'group_name', 'has_uuids', 'number_of_uuids', 'directory',
       'number_of_files'],
      dtype='object')

	if ( datum['group_name'] == 'University of California San Diego TMC' ) or \
		( data['group_name'] == 'Broad Institute RTI' and datum['data_type'] == 'Slide-seq' ):
		return 'phs002249'
	elif datum['group_name'] == 'Stanford TMC':
		return 'phs002272'
	else:
		return None

for index, datum in report.iterrows():
	if not datum['has_uuids']:
		hid = str(datum['hubmap_id'])
		number = str(hubmapbags.uuids.get_number_of_uuids(datum['hubmap_id'], instance='prod', token=token))
		should = str(hubmapbags.uuids.should_i_generate_uuids(datum['hubmap_id'], instance=instance, token=token))
		print( hid + ':' + number + ':' + should )
