from datetime import datetime
from pathlib import Path

import hubmapbags

token = "<this-is-my-token>"

output_directory = "cfdebdbags-" + datetime.today().strftime("%Y%m%d")
if not Path(output_directory).exists():
    Path(output_directory).mkdir(parents=True, exist_ok=True)

if Path("/hive/hubmap-test/bdbags/2022.2").exists():
    output_directory = "/hive/hubmap-test/bdbags/2022.2"
else:
    output_directory = None

id = "HBM666.FFFW.363"
dbgap_study_id = "phs002267"

hubmapbags.magic.do_it(
    id,
    dbgap_study_id=dbgap_study_id,
    copy_output_to=output_directory,
    instance="prod",
    debug=True,
)
