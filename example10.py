import hubmapbags

token='Ag5n60Mdpe8eGkjK8byx4n1YzP51MaEl13Qpxk48vBelzvMm1qfnCOnwY4lED7y3r6jXaMyDdpB5OEFQxzENgTv4Kj'
output_directory = '2022.3'

hubmap_id = 'HBM494.XDQW.356'

answer = hubmapbags.magic.do_it( hubmap_id, \
       			overwrite=False, \
       			dbgap_study_id=None, \
       			token=token, \
       			compute_uuids=False, \
			build_bags=False, \
       			copy_output_to = output_directory, \
   			instance='prod', \
      			debug=True )
