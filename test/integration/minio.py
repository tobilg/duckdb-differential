#!/usr/bin/env python3
"""Disposable pinned MinIO endpoint. No existing containers/data or cloud resources used."""
import json
import argparse
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

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--merge-branches-only',action='store_true',help='SDK fixture plus S3 branch/target merge acceptance only')
parser.add_argument('--merge-only',action='store_true',help='SDK fixture plus S3 snapshot merge acceptance only')
parser.add_argument('--merge-benchmark',action='store_true',help='SDK fixture plus bounded S3 merge benchmark only')
parser.add_argument('--merge-target-benchmark',action='store_true',help='SDK fixture plus bounded S3 existing-target merge benchmark only')
parser.add_argument('--merge-schema-benchmark',action='store_true',help='SDK fixture plus bounded additive schema merge benchmark only')
parser.add_argument('--merge-qualification-only',action='store_true',help='SDK fixture plus candidate-linked S3 pilot, native soak and compatibility gate')
parser.add_argument('--merge-native-spill-only',action='store_true',help='native ARRAY/STRUCT/LIST spill faults and remote recovery verification')
parser.add_argument('--merge-native-scale-case',action='store_true',help='one bounded native scale sample; the matrix driver uses a fresh provider per sample')
parser.add_argument('--scale-rows',type=int,choices=(1000,100000,1000000),default=100000)
parser.add_argument('--scale-percent',type=int,choices=(1,20),default=20)
parser.add_argument('--scale-report',type=Path)
args=parser.parse_args()

with tempfile.TemporaryDirectory(prefix='differential-minio-suites-') as tmp, minio_server() as (endpoint, credentials, container):
    version=docker('exec',container,'minio','--version')
    env=dict(credentials,DIFFERENTIAL_ENABLE_PROBES='1')
    result=subprocess.run([str(TEST_BINARIES/'s3_test'),endpoint,tmp],env=env,text=True,capture_output=True,timeout=60)
    print(result.stdout,end='')
    if result.returncode:raise RuntimeError(result.stderr)
    def qualification():
        output=evidence_path('s3-merge-qualification')
        subprocess.run([sys.executable,str(ROOT/'scripts/qualify_merge.py'),'--s3-location','s3://differential-sdk-test/qualification/',
                        '--s3-endpoint',endpoint,'--s3-path-style','--s3-allow-http','--timeout-seconds','900',
                        '--output',str(output)],env=env,check=True,timeout=960)
        report=json.loads((output/'latest.json').read_text())
        assert report['status']=='passed' and report['storage']=='s3' and not report['release_ready']
        from merge_qualification import check_s3_receipts
        assert check_s3_receipts(report)==5
        print('PASS S3 qualification rejects 5 incomplete or local-only receipt variants',flush=True)
        print('PASS S3 qualification: pilot, native stateful soak and immutable local compatibility; no release approval',flush=True)
    def native_spill():
        subprocess.run([sys.executable,str(ROOT/'test/integration/merge_native_spill.py'),
                        '--s3-location','s3://differential-sdk-test/native-spill/',
                        '--s3-endpoint',endpoint,'--s3-path-style','--s3-allow-http',
                        '--report',str(evidence_path('s3-merge-native-spill.json'))],env=env,check=True,timeout=3660)
    if args.merge_native_spill_only:
        native_spill()
        raise SystemExit(0)
    if args.merge_native_scale_case:
        output=args.scale_report or evidence_path('s3-merge-native-scale-case.json')
        subprocess.run([sys.executable,str(ROOT/'benchmarks/merge_native_scale.py'),
                        '--sizes',str(args.scale_rows),'--percent',str(args.scale_percent),'--repeats','1',
                        '--s3-location','s3://differential-sdk-test/native-scale/',
                        '--s3-endpoint',endpoint,'--s3-path-style','--s3-allow-http','--report',str(output)],
                       env=env,check=True,timeout=960)
        report=json.loads(output.read_text())
        assert report['status']=='passed' and len(report['cases'])==1
        report['provider']={'kind':'MinIO','image':IMAGE,'version':version,'pool_limit_bytes':1073741824,
                            'fresh_container':True,'object_cleanup':'container removed after sample'}
        output.write_text(json.dumps(report,indent=2)+'\n')
        print('PASS native scale sample: fresh bounded MinIO, exact local/remote result and replay',flush=True)
        raise SystemExit(0)
    if args.merge_qualification_only:
        qualification()
        raise SystemExit(0)
    if args.merge_branches_only:
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_branches.py'),endpoint,tmp],env=env,check=True,timeout=900)
        subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_types.py'),endpoint,tmp],env=env,check=True,timeout=300)
        raise SystemExit(0)
    if args.merge_only or args.merge_benchmark or args.merge_schema_benchmark or args.merge_target_benchmark:
        script=ROOT/('benchmarks/s3_merge.py' if args.merge_benchmark or args.merge_schema_benchmark or args.merge_target_benchmark else 'test/integration/s3_merge.py')
        arguments=['--target'] if args.merge_target_benchmark else ['--additive'] if args.merge_schema_benchmark else []
        subprocess.run([sys.executable,str(script),endpoint,tmp,*arguments],env=env,check=True,timeout=900)
        if args.merge_only:
            subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_types.py'),endpoint,tmp],env=env,check=True,timeout=300)
            subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_lifecycle.py'),endpoint,tmp],env=env,check=True,timeout=900)
            subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_branches.py'),endpoint,tmp],env=env,check=True,timeout=900)
            subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_schema.py'),endpoint,tmp],env=env,check=True,timeout=900)
            subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_tables.py'),endpoint,tmp],env=env,check=True,timeout=900)
            subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_catalog.py'),endpoint,tmp],env=env,check=True,timeout=900)
            subprocess.run([sys.executable,str(ROOT/'test/integration/s3_merge_columns.py'),endpoint,tmp],env=env,check=True,timeout=900)
            # Each heavy workload keeps its immutable objects: give it a fresh 1 GiB provider.
            for mode in ('--merge-native-spill-only','--merge-qualification-only'):
                subprocess.run([sys.executable,str(Path(__file__).resolve()),mode],check=True,timeout=7200)
        raise SystemExit(0)  # finally still removes this disposable container
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
    # Merge suites run in their own disposable provider: every suite keeps its immutable objects,
    # and the merge workloads together exceed what remains of this container's 1 GiB pool.
    subprocess.run([sys.executable,str(Path(__file__).resolve()),'--merge-only'],check=True,timeout=7200)
    if os.environ.get('DIFFERENTIAL_S3_BENCHMARK'):
        subprocess.run([sys.executable,str(ROOT/'benchmarks/s3.py'),endpoint,tmp],env=env,check=True,timeout=240)
    evidence_path('minio-sdk-results.json').write_text(json.dumps({'image':IMAGE,'version':version,'command':'python3 test/integration/minio.py','build_directory':str(BUILD_DIR),'extension_override':os.environ.get('DIFFERENTIAL_EXTENSION',''),'result':result.stdout,'scope':'SDK transport portion; snapshot, publication-fault and resource evidence recorded separately; no real AWS verification'},indent=2)+'\n')
