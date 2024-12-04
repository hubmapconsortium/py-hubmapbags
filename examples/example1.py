from datetime import datetime
from pathlib import Path
import hubmapbags
from os import getenv
from sys import exit

token = getenv("TOKEN")
if token is None:
    print("Error: TOKEN environment variable is not set")
    exit(1)

hubmap_id = "HBM666.FFFW.363"
inventory_directory = "/Users/icaoberg/Documents/submission/inventory"

hubmapbags.magic.do_it(
    hubmap_id,
    token=token,
    build_bags=True,
    inventory_directory=inventory_directory,
    debug=True,
)
