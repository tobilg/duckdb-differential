#!/usr/bin/env python3
"""Disposable pinned MinIO endpoint. No existing containers/data or cloud resources used."""
import json
import os
from pathlib import Path
import secrets
import sys
import subprocess
import tempfile
import time
import urllib.request
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

# Upstream RELEASE.2025-10-15T17-29-55Z, built by https://github.com/coollabsio/minio.
# Pin the multi-platform index (linux/amd64 + linux/arm64); keep Integration.yml in sync.
IMAGE='ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9'

def docker(*args):
    return subprocess.run(['docker',*args],check=True,text=True,capture_output=True).stdout.strip()

with tempfile.TemporaryDirectory(prefix='differential-minio-') as tmp:
    folder=Path(tmp);username='test_'+secrets.token_hex(8);password=secrets.token_hex(24)
    config=folder/'credentials.env';config.write_text(f'MINIO_ROOT_USER={username}\nMINIO_ROOT_PASSWORD={password}\n');config.chmod(0o600)
    container=docker('run','--pull=never','--rm','-d','--name','differential-sdk-'+secrets.token_hex(6),
                     '--env-file',str(config),'-p','127.0.0.1::9000','--tmpfs','/data:rw,size=512m,mode=1777',IMAGE,'server','/data')
    try:
        address=docker('port',container,'9000/tcp');endpoint='http://'+address
        deadline=time.monotonic()+30
        while True:
            try:
                with urllib.request.urlopen(endpoint+'/minio/health/live',timeout=1) as response:
                    if response.status==200:break
            except Exception:
                if time.monotonic()>deadline:raise RuntimeError('MinIO startup timeout')
                time.sleep(.2)
        version=docker('exec',container,'minio','--version')
        env=dict(os.environ,DIFFERENTIAL_ENABLE_PROBES='1',AWS_ACCESS_KEY_ID=username,AWS_SECRET_ACCESS_KEY=password,AWS_EC2_METADATA_DISABLED='true',AWS_DEFAULT_REGION='us-east-1')
        result=subprocess.run([str(TEST_BINARIES/'s3_test'),endpoint,tmp],env=env,text=True,capture_output=True,timeout=60)
        print(result.stdout,end='')
        if result.returncode:raise RuntimeError(result.stderr)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_product.py'),endpoint,tmp],env=env,check=True,timeout=180)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_publication_faults.py'),endpoint,tmp],env=env,check=True,timeout=240)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_resources.py'),endpoint,tmp],env=env,check=True,timeout=240)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_tiering.py'),endpoint,tmp],env=env,check=True,timeout=300)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_heads.py'),endpoint,tmp],env=env,check=True,timeout=600)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_layout.py'),endpoint,tmp],env=env,check=True,timeout=600)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_shared.py'),endpoint,tmp],env=env,check=True,timeout=900)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_shared_faults.py'),endpoint,tmp],env=env,check=True,timeout=900)
        subprocess.run([sys.executable,str(ROOT/'test/integration/doc_examples.py'),endpoint,tmp],env=env,check=True,timeout=900)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_secrets.py'),endpoint,tmp],env=env,check=True,timeout=900)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_cache.py'),endpoint,tmp],env=env,check=True,timeout=900)
        subprocess.run([sys.executable,str(ROOT/'test/integration/coexistence.py'),endpoint,tmp],env=env,check=True,timeout=600)
        subprocess.run([sys.executable,str(ROOT/'examples/s3.py'),'--bucket','differential-sdk-test','--prefix','example-test/','--endpoint',endpoint,'--path-style','--allow-http'],env=env,check=True,timeout=120)
        subprocess.run([sys.executable,str(ROOT/'test/integration/existing_provider.py'),'--bucket','differential-sdk-test','--prefix','capability-test/','--endpoint',endpoint,'--path-style','--allow-http','--report',str(evidence_path('existing-provider-minio.json'))],env=env,check=True,timeout=300)
        if os.environ.get('DIFFERENTIAL_S3_BENCHMARK'):
            subprocess.run([sys.executable,str(ROOT/'benchmarks/s3.py'),endpoint,tmp],env=env,check=True,timeout=240)
        evidence_path('minio-sdk-results.json').write_text(json.dumps({'image':IMAGE,'version':version,'command':'python3 test/integration/minio.py','build_directory':str(BUILD_DIR),'extension_override':os.environ.get('DIFFERENTIAL_EXTENSION',''),'result':result.stdout,'scope':'SDK transport portion; snapshot, publication-fault and resource evidence recorded separately; no real AWS verification'},indent=2)+'\n')
    finally:
        docker('rm','-f',container)
