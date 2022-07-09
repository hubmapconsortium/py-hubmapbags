import hubmapbags

token='Ag5n60Mdpe8eGkjK8byx4n1YzP51MaEl13Qpxk48vBelzvMm1qfnCOnwY4lED7y3r6jXaMyDdpB5OEFQxzENgTv4Kj'
output_directory = '2022.3'

assays = ['IMC2D','DART-FISH', \
		'Lightsheet','MxIF', \
		'PAS','MIBI','cell-dive', \
		'bulk-RNA', 'SNARE-ATACseq2', 'SNARE-RNAseq2', \
		'snATACseq', 'snRNAseq-10xGenomics-v2', 'snRNAseq-10xGenomics-v3', 'Slide-seq', 'Targeted-Shotgun-LC-MS', 'TMT-LC-MS', 'WGS', 'LC-MS', 'MS', 'LC-MS_bottom_up', 'MS_bottom_up', \
		'LC-MS_bottom_up', 'MS_bottom_up', 'LC-MS_top_down', 'MS_top_down']

assays = ['AF', 'ATACseq-bulk', 'cell-dive', 'CODEX', 'DART-FISH', 'IMC2D', 'IMC3D', \
	'lc-ms_label-free', 'lc-ms_labeled', 'lc-ms-ms_label-free', 'lc-ms-ms_labeled', \
	'LC-MS-untargeted', 'Lightsheet', 'MALDI-IMS', 'MIBI', 'NanoDESI', 'NanoPOTS', \
	'MxIF', 'PAS', 'bulk-RNA', 'SNARE-ATACseq2', 'SNARE-RNAseq2', 'scRNAseq-10xGenomics-v2', \
	'scRNAseq-10xGenomics-v3', 'sciATACseq', 'sciRNAseq', 'seqFish', \
	'snATACseq', 'snRNAseq-10xGenomics-v2', 'snRNAseq-10xGenomics-v3', 'Slide-seq', \
	'Targeted-Shotgun-LC-MS', 'TMT-LC-MS', 'WGS', 'LC-MS', 'MS', 'LC-MS_bottom_up', \
	'MS_bottom_up', 'LC-MS_top_down', 'MS_top_down']


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
				build_bags=False, \
        			copy_output_to = output_directory, \
        			instance='prod', \
        			debug=True )
