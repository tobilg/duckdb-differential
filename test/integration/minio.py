#!/usr/bin/env python3
"""Disposable pinned MinIO endpoint. No existing containers/data or cloud resources used."""
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

from minio_server import IMAGE, docker, minio_server

with tempfile.TemporaryDirectory(prefix='differential-minio-suites-') as tmp, minio_server() as (endpoint, credentials, container):
    version=docker('exec',container,'minio','--version')
    env=dict(credentials,DIFFERENTIAL_ENABLE_PROBES='1')
    result=subprocess.run([str(TEST_BINARIES/'s3_test'),endpoint,tmp],env=env,text=True,capture_output=True,timeout=60)
    print(result.stdout,end='')
    if result.returncode:raise RuntimeError(result.stderr)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_product.py'),endpoint,tmp],env=env,check=True,timeout=180)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_publication_faults.py'),endpoint,tmp],env=env,check=True,timeout=240)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_resources.py'),endpoint,tmp],env=env,check=True,timeout=240)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_tiering.py'),endpoint,tmp],env=env,check=True,timeout=300)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_heads.py'),endpoint,tmp],env=env,check=True,timeout=600)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_gc.py'),endpoint,tmp],env=env,check=True,timeout=900)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_layout.py'),endpoint,tmp],env=env,check=True,timeout=600)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_shared.py'),endpoint,tmp],env=env,check=True,timeout=900)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_shared_faults.py'),endpoint,tmp],env=env,check=True,timeout=900)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_shipping.py'),endpoint,tmp],env=env,check=True,timeout=1800)
    subprocess.run([sys.executable,str(ROOT/'test/integration/doc_examples.py'),endpoint,tmp],env=env,check=True,timeout=900)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_secrets.py'),endpoint,tmp],env=env,check=True,timeout=900)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_cache.py'),endpoint,tmp],env=env,check=True,timeout=900)
    subprocess.run([sys.executable,str(ROOT/'test/integration/s3_prefetch.py'),endpoint,tmp],env=env,check=True,timeout=900)
    subprocess.run([sys.executable,str(ROOT/'test/integration/coexistence.py'),endpoint,tmp],env=env,check=True,timeout=600)
    subprocess.run([sys.executable,str(ROOT/'examples/s3.py'),'--bucket','differential-sdk-test','--prefix','example-test/','--endpoint',endpoint,'--path-style','--allow-http'],env=env,check=True,timeout=120)
    subprocess.run([sys.executable,str(ROOT/'test/integration/existing_provider.py'),'--bucket','differential-sdk-test','--prefix','capability-test/','--endpoint',endpoint,'--path-style','--allow-http','--report',str(evidence_path('existing-provider-minio.json'))],env=env,check=True,timeout=300)
    if os.environ.get('DIFFERENTIAL_S3_BENCHMARK'):
        subprocess.run([sys.executable,str(ROOT/'benchmarks/s3.py'),endpoint,tmp],env=env,check=True,timeout=240)
    evidence_path('minio-sdk-results.json').write_text(json.dumps({'image':IMAGE,'version':version,'command':'python3 test/integration/minio.py','build_directory':str(BUILD_DIR),'extension_override':os.environ.get('DIFFERENTIAL_EXTENSION',''),'result':result.stdout,'scope':'SDK transport portion; snapshot, publication-fault and resource evidence recorded separately; no real AWS verification'},indent=2)+'\n')
