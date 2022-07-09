import hubmapbags

token='AgmJblmbNeNyN5nJdqYNWN9mEOjJdEgy4pmj2Oq5bX4NY598ept8CN3q2Pp3XwGqV6jJBqbJYKVGM3F63Q7MqCVjoy'
output_directory = '2022.3'

assays = ['CODEX']
assays = ['ATACseq-bulk', 'cell-dive', 'IMC2D', 'IMC3D']

for assay in assays:
	ids = hubmapbags.apis.get_hubmap_ids( assay, token=token )

	for id in ids:
		hubmap_id = id['hubmap_id']

		if id['data_type'] == assay:
			answer = hubmapbags.magic.do_it( hubmap_id, \
        			overwrite=False, \
        			dbgap_study_id=None, \
        			token=token, \
        			compute_uuids=False, \
				build_bags=True, \
        			copy_output_to = output_directory, \
        			instance='prod', \
        			debug=True )
