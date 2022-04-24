import hubmapbags
from datetime import datetime
from pathlib import Path

output_directory = '/hive/hubmap-test/bdbags/2022.2'

id = 'HBM666.FFFW.363'
token = '<this-is-my-token>'
instance = 'prod' #default instance is test

uuids = hubmapbags.uuids.get_uuids( id, token=token, instance=instance )

print('Querying uuid-api')
print( uuids )
