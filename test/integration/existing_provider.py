#!/usr/bin/env python3
"""Verify an explicitly supplied, authorized existing S3 namespace using the SDK.

Allocates a random child prefix; never creates infrastructure. The transport probe
deletes only the two objects it creates. The public example export remains retained.
Run only with an existing authorized bucket/prefix and SDK credentials configured
outside this repository. Compatible endpoints and AWS are recorded distinctly.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--bucket', required=True)
p.add_argument('--prefix', required=True)
p.add_argument('--region', default='us-east-1')
p.add_argument('--profile', default='')
p.add_argument('--endpoint', default='')
p.add_argument('--path-style', action='store_true')
p.add_argument('--allow-http', action='store_true')
p.add_argument('--report', type=Path, required=True)
args = p.parse_args()
if not args.prefix.strip('/'):
    p.error('Supply a nonempty authorized test prefix')
prefix = args.prefix.rstrip('/') + '/verification-' + secrets.token_hex(12) + '/'
build = BUILD_DIR
extension = EXTENSION
env = dict(os.environ, DIFFERENTIAL_TEST_EXISTING_BUCKET=args.bucket,
           DIFFERENTIAL_TEST_PREFIX=prefix+'transport/', DIFFERENTIAL_TEST_REGION=args.region,
           DIFFERENTIAL_TEST_PROFILE=args.profile, DIFFERENTIAL_TEST_PATH_STYLE=str(int(args.path_style)),
           DIFFERENTIAL_TEST_ALLOW_HTTP=str(int(args.allow_http)))
with tempfile.TemporaryDirectory(prefix='differential-existing-provider-') as tmp:
    result = subprocess.run([str(TEST_BINARIES / 's3_test'), args.endpoint, tmp], env=env,
                            check=True, text=True, capture_output=True, timeout=180)
    print(result.stdout, end='')
    command = [sys.executable, str(ROOT / 'examples/s3.py'), '--bucket', args.bucket,
               '--prefix', prefix+'exports/', '--region', args.region,
               '--profile', args.profile, '--endpoint', args.endpoint]
    if args.path_style:
        command.append('--path-style')
    if args.allow_http:
        command.append('--allow-http')
    example = subprocess.run(command, env=env, check=True, text=True, capture_output=True, timeout=240)
    exported = json.loads(example.stdout)
report = {
    'platform': platform.platform(), 'provider': 'AWS S3' if not args.endpoint else 'explicit compatible endpoint',
    'endpoint': args.endpoint, 'bucket': args.bucket, 'prefix': prefix, 'region': args.region,
    'extension_sha256': hashlib.sha256(extension.read_bytes()).hexdigest(),
    'transport': result.stdout.splitlines(), 'self_contained_export': exported,
    'scope': 'SDK conditional PUT/duplicate, multipart/abort, HEAD, exact range/checksum, LIST, owned DELETE, missing object; public export/fresh-process read after writer catalog removal. No physical second-host verification. Export graph remains retained. Fault harness is a separate suite.',
    'command': sys.argv
}
args.report.write_text(json.dumps(report, indent=2)+'\n')
print('PASS existing-provider capability and independent snapshot-reader suite; export retained')
