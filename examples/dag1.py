import hubmapbags
from os import getenv
from sys import exit

token = getenv("TOKEN")
if token is None:
    print("Error: TOKEN environment variable is not set")
    exit(1)

hubmapbags.utilities.clean()
hubmapbags.reports.daily(token=token)
