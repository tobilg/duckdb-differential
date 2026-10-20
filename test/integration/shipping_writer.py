#!/usr/bin/env python3
"""Test helper for s3_shipping.py: runs SQL statements in one DuckDB instance on a repository and
prints each result (first cell) as a JSON line; "!sleep <seconds>" pauses. With kill=1 the process exits without detaching or
closing anything, as if it was killed."""
import json
import os
import sys
import time
from paths import EXTENSION
from stock_capi import Host

repository, kill, *statements = sys.argv[1:]
host = Host()
conn = host.connect()
host.query(conn, f"LOAD '{EXTENSION}'")
host.query(conn, f"SET differential_repository = '{repository}'")
for statement in statements:
    if statement.startswith('!sleep '):
        time.sleep(float(statement.split()[1]))
        continue
    try:
        print(json.dumps({'ok': host.query(conn, statement)}), flush=True)
    except RuntimeError as error:
        print(json.dumps({'error': str(error)}), flush=True)
        if kill == '1':
            os._exit(3)
        sys.exit(3)
if kill == '1':
    os._exit(0)
host.close()
