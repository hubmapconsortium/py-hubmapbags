import hubmapbags
from datetime import datetime
from pathlib import Path

output_directory = '/hive/hubmap-test/bdbags/2022.2'

ids=['HBM775.RVQX.376',
 'HBM599.GLRZ.888',
 'HBM233.CCCX.767',
 'HBM837.FPWJ.865',
 'HBM743.FBJP.586',
 'HBM938.GJXR.224',
 'HBM756.PWKH.456',
 'HBM655.MFTK.764',
 'HBM659.GSQR.225',
 'HBM558.BHPZ.328',
 'HBM233.XQZM.395',
 'HBM453.GWNF.247',
 'HBM987.BFBR.496',
 'HBM367.ZMBH.758',
 'HBM557.VZPM.253',
 'HBM949.PNXL.623',
 'HBM367.NSZK.788',
 'HBM889.DMLC.292',
 'HBM433.SPRB.778',
 'HBM243.MXBM.589',
 'HBM925.FQDP.328',
 'HBM684.SLGB.599',
 'HBM657.XWQQ.636',
 'HBM373.FZMG.625',
 'HBM379.PCLL.836',
 'HBM373.VTNH.683',
 'HBM879.DFQN.248',
 'HBM638.CDHV.585',
 'HBM477.KVFD.827',
 'HBM545.QLKW.543',
 'HBM247.JTNN.859',
 'HBM599.CXNC.464',
 'HBM322.TNGF.859',
 'HBM745.GCNN.553',
 'HBM439.LWSZ.467',
 'HBM346.LSFW.324',
 'HBM569.FMVR.429',
 'HBM892.VLVC.242',
 'HBM399.GZRJ.726',
 'HBM487.WJST.938',
 'HBM354.FMKQ.822',
 'HBM958.VZLG.297',
 'HBM324.MKDC.693',
 'HBM337.GJHZ.665',
 'HBM272.KQDJ.873',
 'HBM684.XVPK.336',
 'HBM489.LPCX.978',
 'HBM832.WWZH.575',
 'HBM754.SJMP.486',
 'HBM586.QXWD.492',
 'HBM797.HSMJ.596',
 'HBM387.KXLD.867',
 'HBM246.PZPG.573',
 'HBM499.GPFC.894',
 'HBM258.RBCM.393',
 'HBM973.XRLR.365',
 'HBM699.CHMK.457',
 'HBM543.DWSW.978',
 'HBM348.FXGT.728',
 'HBM969.VBPS.239',
 'HBM876.NMKV.392']

token='<this-is-my-token>'

for id in ids:
	hubmapbags.magic.do_it( id, \
        overwrite=True, \ #will re-compute checksums even if local file is present
        dbgap_study_id=None, \
        token=token, \
        compute_uuids=True, \ #will compute uuids unless it finds ids in the uuid-api database
        copy_output_to = output_directory, \
        instance='prod', \
        debug=True )

