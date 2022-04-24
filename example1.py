import hubmapbags
from datetime import datetime
from pathlib import Path

export TOKEN='<this-is-my-token>'

output_directory = 'cfdebdbags-' + datetime.today().strftime('%Y%m%d')
if not Path(output_directory).exists():
	Path(output_directory).mkdir(parents=True, exist_ok=True)

output_directory = '/hive/hubmap-test/bdbags/2022.2'

id = 'HBM666.FFFW.363'
dbgap_study_id = 'phs002267'

hubmapbags.magic.do_it( id, \
        overwrite=False, \ #
	dbgap_study_id=dbgap_study_id, \
        compute_uuids=False, \
        copy_output_to = output_directory, \
        instance='prod', \
        debug=True )

uuids = hubmapbags.uuids.get_uuids( id, token=token )
print('Querying the uuid-api')
print( uuids )
