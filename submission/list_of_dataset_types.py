import hubmapbags
import os

token = os.getenv("TOKEN")
assays = hubmapbags.apis.get_assay_types(token)
print(assays)
