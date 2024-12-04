from pathlib import Path
import hubmapbags
from datetime import datetime
from os import getenv
from sys import exit

token = getenv("TOKEN")
if token is None:
    print("Error: TOKEN environment variable is not set")
    exit(1)

#set this variable to your local copy of the inventory files
inventory_directory = "/Users/icaoberg/Documents/submission/inventory"

start_time = datetime.now()

hubmapbags.magic.create_submission(
    token=token,
    build_bags=True,
    inventory_directory=inventory_directory,
    debug=True,
)

end_time = datetime.now()
duration = end_time - start_time

# Print the duration
print(f"Time taken to build was {duration}")
