# README

## Examples

### Get list of assay names
``
import hubmapbags
from datetime import datetime
from pathlib import Path

answer = hubmapbags.apis.get_assay_types()
print(answer)
``

will result in something like

```
['AF', 'ATACseq-bulk', 'cell-dive', 'CODEX', 'DART-FISH', 'IMC2D', 'IMC3D', 'lc-ms_label-free', 'lc-ms_labeled', 'lc-ms-ms_label-free', 'lc-ms-ms_labeled', 'LC-MS-untargeted', 'Lightsheet', 'MALDI-IMS', 'MIBI', 'NanoDESI', 'NanoPOTS', 'MxIF', 'PAS', 'bulk-RNA', 'SNARE-ATACseq2', 'SNARE-RNAseq2', 'scRNAseq-10xGenomics-v2', 'scRNAseq-10xGenomics-v3', 'sciATACseq', 'sciRNAseq', 'seqFish', 'seqFish_pyramid', 'snATACseq', 'snRNAseq-10xGenomics-v2', 'snRNAseq-10xGenomics-v3', 'Slide-seq', 'Targeted-Shotgun-LC-MS', 'TMT-LC-MS', 'WGS', 'LC-MS', 'MS', 'LC-MS_bottom_up', 'MS_bottom_up', 'LC-MS_top_down', 'MS_top_down']
```

### Get list of HuBMAP ids, UUIDs and status given an assay name

```
import hubmapbags
from datetime import datetime
from pathlib import Path

assay_name = 'AF'
answer = hubmapbags.apis.get_hubmap_ids( assay_name=assay_name )
print(answer)
```

will result in something like

```
[{'uuid': '2f9e91ff774243ef11d148a2bf7a6822', 'hubmap_id': 'HBM268.FSPK.489', 'status': 'Published'}, {'uuid': 'dc289471333309925e46ceb9bafafaf4', 'hubmap_id': 'HBM279.SSXF.866', 'status': 'Published'}, {'uuid': '2bc6713576dda7c7e6378aec38df437d', 'hubmap_id': 'HBM447.NXPZ.263', 'status': 'Published'}, {'uuid': '57299edf509b218aa9b4c4a2e1d979bd', 'hubmap_id': 'HBM535.QCJS.935', 'status': 'Published'}, {'uuid': '02e13b9b3cdc939cca397c42c2981dd1', 'hubmap_id': 'HBM362.DLZS.564', 'status': 'Published'}, {'uuid': '46ed471a6b4ad5b97817ff2c26ef6ddd', 'hubmap_id': 'HBM873.FRQB.759', 'status': 'Published'}, {'uuid': '109162ed40ede274202eab96bf640fa4', 'hubmap_id': 'HBM739.JSGQ.673', 'status': 'Published'}, {'uuid': '5eb7d04d71566c53dfc9eb1c7346c68d', 'hubmap_id': 'HBM882.DMQM.597', 'status': 'Published'}, {'uuid': 'f54b458ca42f7112d0e0751c9ba41492', 'hubmap_id': 'HBM298.JRGF.528', 'status': 'Published'}, {'uuid': '6a037ddcb811f77f726b9f78e5d369a2', 'hubmap_id': 'HBM969.JXDC.887', 'status': 'Published'}]
```
