from datetime import datetime
from pathlib import Path
import hubmapbags

token = ""

hubmap_id = "HBM759.JWGJ.636"
inventory_directory = "/Users/icaoberg/Documents/submission/inventory"

hubmapbags.magic.do_it(
    hubmap_id,
    token=token,
    build_bags=True,
    inventory_directory=inventory_directory,
    debug=True,
)
