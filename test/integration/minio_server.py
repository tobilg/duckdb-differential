"""A disposable, pinned MinIO container for the S3 suites and benchmarks.

`minio_server()` starts it with generated credentials on a loopback port and yields
(endpoint, credentials environment). Data lives on a tmpfs by default; benchmarks pass a host
directory (`data_dir`) for data sets larger than memory. `create_bucket()` creates a bucket with a
SigV4-signed request (the suites otherwise create it through the SDK probe of `make integration`)."""
from contextlib import contextmanager
import datetime
import hashlib
import hmac
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

# Upstream RELEASE.2025-10-15T17-29-55Z, built by https://github.com/coollabsio/minio.
# Pin the multi-platform index (linux/amd64 + linux/arm64); keep Integration.yml in sync.
IMAGE = 'ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9'


def docker(*args):
    return subprocess.run(['docker', *args], check=True, text=True, capture_output=True).stdout.strip()


@contextmanager
def minio_server(data_dir=None):
    with tempfile.TemporaryDirectory(prefix='differential-minio-') as tmp:
        username, password = 'test_' + secrets.token_hex(8), secrets.token_hex(24)
        config = Path(tmp) / 'credentials.env'
        config.write_text(f'MINIO_ROOT_USER={username}\nMINIO_ROOT_PASSWORD={password}\n')
        config.chmod(0o600)
        storage = ['-v', f'{data_dir}:/data'] if data_dir else ['--tmpfs', '/data:rw,size=1g,mode=1777']
        container = docker('run', '--pull=never', '--rm', '-d', '--name', 'differential-sdk-' + secrets.token_hex(6),
                           '--env-file', str(config), '-p', '127.0.0.1::9000', *storage, IMAGE, 'server', '/data')
        try:
            endpoint = 'http://' + docker('port', container, '9000/tcp')
            deadline = time.monotonic() + 30
            while True:
                try:
                    with urllib.request.urlopen(endpoint + '/minio/health/live', timeout=1) as response:
                        if response.status == 200:
                            break
                except Exception:
                    if time.monotonic() > deadline:
                        raise RuntimeError('MinIO startup timeout')
                    time.sleep(.2)
            env = dict(os.environ, AWS_ACCESS_KEY_ID=username, AWS_SECRET_ACCESS_KEY=password,
                       AWS_EC2_METADATA_DISABLED='true', AWS_DEFAULT_REGION='us-east-1')
            yield endpoint, env, container
        finally:
            subprocess.run(['docker', 'rm', '-f', container], capture_output=True)


def create_bucket(endpoint, env, bucket, region='us-east-1'):
    """PUT /<bucket>, retried while MinIO is live but not yet ready (503)."""
    deadline = time.monotonic() + 60
    while True:
        try:
            return _put_bucket(endpoint, env, bucket, region)
        except urllib.error.HTTPError as error:
            if error.code != 503 or time.monotonic() > deadline:
                raise
            time.sleep(.5)


def _put_bucket(endpoint, env, bucket, region):
    """PUT /<bucket> signed with AWS Signature Version 4 (path style)."""
    host = endpoint.split('://', 1)[1]
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
    payload = hashlib.sha256(b'').hexdigest()
    headers = {'host': host, 'x-amz-content-sha256': payload, 'x-amz-date': stamp}
    signed = ';'.join(sorted(headers))
    canonical = '\n'.join(['PUT', f'/{bucket}', '', ''.join(f'{k}:{headers[k]}\n' for k in sorted(headers)), signed,
                           payload])
    scope = f'{day}/{region}/s3/aws4_request'
    to_sign = '\n'.join(['AWS4-HMAC-SHA256', stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = ('AWS4' + env['AWS_SECRET_ACCESS_KEY']).encode()
    for part in (day, region, 's3', 'aws4_request'):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(f'{endpoint}/{bucket}', method='PUT', headers={
        'x-amz-content-sha256': payload, 'x-amz-date': stamp,
        'Authorization': f"AWS4-HMAC-SHA256 Credential={env['AWS_ACCESS_KEY_ID']}/{scope}, SignedHeaders={signed}, "
                         f'Signature={signature}'})
    with urllib.request.urlopen(request, timeout=10) as response:
        assert response.status == 200, response.status


def put_object(endpoint, env, bucket, key, path, region='us-east-1'):
    """PUT /<bucket>/<key> with the file's bytes (SigV4, unsigned payload; benchmarks only)."""
    host = endpoint.split('://', 1)[1]
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
    payload = 'UNSIGNED-PAYLOAD'
    headers = {'host': host, 'x-amz-content-sha256': payload, 'x-amz-date': stamp}
    signed = ';'.join(sorted(headers))
    canonical = '\n'.join(['PUT', f'/{bucket}/{key}', '', ''.join(f'{k}:{headers[k]}\n' for k in sorted(headers)),
                           signed, payload])
    scope = f'{day}/{region}/s3/aws4_request'
    to_sign = '\n'.join(['AWS4-HMAC-SHA256', stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    signing = ('AWS4' + env['AWS_SECRET_ACCESS_KEY']).encode()
    for part in (day, region, 's3', 'aws4_request'):
        signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing, to_sign.encode(), hashlib.sha256).hexdigest()
    size = os.path.getsize(path)
    with open(path, 'rb') as body:
        request = urllib.request.Request(f'{endpoint}/{bucket}/{key}', data=body, method='PUT', headers={
            'x-amz-content-sha256': payload, 'x-amz-date': stamp, 'Content-Length': str(size),
            'Authorization': f"AWS4-HMAC-SHA256 Credential={env['AWS_ACCESS_KEY_ID']}/{scope}, "
                             f'SignedHeaders={signed}, Signature={signature}'})
        with urllib.request.urlopen(request, timeout=600) as response:
            assert response.status == 200, response.status
