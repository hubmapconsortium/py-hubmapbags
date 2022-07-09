import hubmapbags

token='Agr6nywq1yw4J7dXBMQlKMx0mnqG7a5pWjK3B7ed939adlJjeqUXC8ayV0V2DbndVwkeVbqk04Gy83f0276VjhKkJ4'
hubmap_id = 'HBM987.XGTH.368' #dataset id
output_directory = '2022.3'

answer = hubmapbags.magic.do_it( hubmap_id, \
        overwrite=False, \
        dbgap_study_id=None, \
        token=token, \
        compute_uuids=False, \
        copy_output_to = output_directory, \
        instance='prod', \
        debug=True )
