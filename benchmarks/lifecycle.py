#!/usr/bin/env python3
"""Same-pool branch/clone metadata cost and payload reuse, with native durability unchanged."""
import json
import os
from pathlib import Path
import platform
import sqlite3
import sys
import tempfile
import time
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'test/integration'))
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

from stock_capi import Host
EXT=EXTENSION
measurements=[]
with tempfile.TemporaryDirectory(prefix='differential-lifecycle-bench-') as tmp:
    folder=Path(tmp);os.environ['DIFFERENTIAL_STORE_ROOT']=tmp;host=Host();c=host.connect()
    try:
        # Measure branch/clone cost alone: collect superseded generations explicitly first,
        # with background collection disabled so payload counts are stable.
        host.query(c,f"LOAD '{EXT}'; SET differential_auto_gc=false; ATTACH 'differential:sales' AS d; CREATE TABLE d.t AS SELECT range AS id,sha256(range::VARCHAR) AS payload FROM range(100000); CALL differential_create_snapshot('d'); CALL differential_cleanup_old_files(max_objects:=10000);")
        def stats():
            with sqlite3.connect(folder/'engine.sqlite') as metadata:
                count,size=metadata.execute('SELECT count(*),sum(length(data)) FROM objects').fetchone()
                extents=metadata.execute('SELECT count(*) FROM extents').fetchone()[0]
            physical=sum(p.stat().st_size for p in folder.iterdir() if p.name.startswith('engine.sqlite'))
            return count,size,extents,physical
        original=stats()
        for kind in ('branch','clone'):
            for i in range(8):
                before=stats();start=time.perf_counter()
                function='differential_create_branch' if kind=='branch' else 'differential_clone_database'
                host.query(c,f"CALL {function}('sales','{kind}{i}',snapshot:=1);")
                elapsed=(time.perf_counter()-start)*1000;after=stats()
                assert after[:2]==before[:2],(kind,before,after)
                measurements.append({'kind':kind,'iteration':i,'milliseconds':elapsed,'new_payload_objects':0,'new_payload_bytes':0,'new_extent_entries':after[2]-before[2],'authority_file_growth':after[3]-before[3]})
        host.query(c,"ATTACH 'differential:clone0' AS child; DELETE FROM child.t;")
        assert host.query(c,'SELECT count(*) FROM d.t;')==100000
        assert host.query(c,'SELECT count(*) FROM child.t;')==0
    finally:host.close()
report={'platform':platform.platform(),'rows':100000,'source_payload_objects':original[0],'source_payload_bytes':original[1],'note':'source payload measured after explicit GC of superseded internal generations','scope':'one source snapshot, eight branches and eight clones, same repository/pool; C API call timings exclude process startup; authority file growth includes SQLite WAL and can shrink on checkpoint','measurements':measurements}
(ROOT/'benchmarks/lifecycle-results.json').write_text(json.dumps(report,indent=2)+'\n')
print('PASS eight branches and eight clones created without duplicating payload; native clone writes isolated')
