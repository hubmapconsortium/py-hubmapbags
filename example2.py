import hubmapbags
from datetime import datetime
from pathlib import Path

output_directory = '/hive/hubmap-test/bdbags/2022.2'

ids=['HBM788.KHPF.569','HBM434.VLFT.986', \
	'HBM965.GZHL.485','HBM552.MSWZ.288', \
	'HBM384.XXGK.466','HBM676.SNSW.596', \
	'HBM766.XTBC.899','HBM682.TWTR.428', \
	'HBM376.FRWD.797']

token='<this-is-my-token>'

for id in ids:
	hubmapbags.magic.do_it( id, \
        	overwrite=True, \ #will re-compute checksums even if local file is present
		dbgap_study_id=None, \
		token=token, \
        	compute_uuids=True, \ #will compute uuids unless it finds ids in the uuid-api database
        	copy_output_to = output_directory, \
        	instance='prod', \
        	debug=True )

