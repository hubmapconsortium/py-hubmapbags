import hubmapbags

token='Ag5n60Mdpe8eGkjK8byx4n1YzP51MaEl13Qpxk48vBelzvMm1qfnCOnwY4lED7y3r6jXaMyDdpB5OEFQxzENgTv4Kj'
output_directory = '2022.3'

assays = hubmapbags.apis.get_assay_types()
print(assays)
