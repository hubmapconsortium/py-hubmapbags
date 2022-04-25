import hubmapbags
from datetime import datetime
from pathlib import Path

id = 'HBM666.FFFW.363'
dbgap_study_id = 'phs002267'
token='Aga5KV8d7n5xavgEMYV9GGWN8547xQ8mrOWbMkbGvlGo945rVNc9CxgXnga8YMpa5OgWz8zmPezPo6FnlYwVQSWG0z'

answer = hubmapbags.uuids.populate_local_file_with_remote_uuids( id, instance='prod', token=token, debug=True )
print(answer)
