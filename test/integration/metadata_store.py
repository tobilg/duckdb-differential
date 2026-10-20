"""Read-only access to a repository's metadata store (engine.sqlite) from a separate process.

The extension links its own SQLite. A second SQLite copy in the same process (Python's sqlite3)
does not see the extension's POSIX locks or its shared-memory WAL index, and can truncate the
index while the extension has it mapped (SIGBUS). Tests that keep an in-process DuckDB host open
must therefore read the store through this module, which runs each query in a child process.
"""
import json
import subprocess
import sys

_CHILD = r'''
import json, sqlite3, sys
request = json.load(sys.stdin)
with sqlite3.connect(request['path']) as db:
    rows = db.execute(request['sql'], request['params']).fetchall()
def encode(value):
    return {'__bytes__': value.hex()} if isinstance(value, bytes) else value
json.dump([[encode(v) for v in row] for row in rows], sys.stdout)
'''


def _decode(value):
    return bytes.fromhex(value['__bytes__']) if isinstance(value, dict) else value


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _Store:
    def __init__(self, path):
        self.path = str(path)

    def execute(self, sql, params=()):
        result = subprocess.run([sys.executable, '-I', '-c', _CHILD], text=True, capture_output=True,
                                input=json.dumps({'path': self.path, 'sql': sql, 'params': list(params)}))
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
        return _Result([tuple(_decode(v) for v in row) for row in json.loads(result.stdout)])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def connect(path):
    """A read-only, sqlite3-like handle on the store at `path` (each execute runs in a child process)."""
    return _Store(path)
