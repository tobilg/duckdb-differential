#!/usr/bin/env python3
"""Repository sharing between local users: private by default (0700/0600), or created for
several users with SET differential_repository_shared = true (setgid group-writable 02770
directories, 0660 files). Modes must not depend on the process umask. Disposable directories
only; actual access by a second OS user is not exercised (needs another account)."""
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
from evidence import evidence_path
from paths import EXTENSION
from stock_capi import Host

results = []


def passed(name):
    results.append(name)
    print('PASS', name, flush=True)


def modes(*roots):
    """{relative path: permission bits} for every directory and file below the roots."""
    found = {}
    for root in roots:
        for path in [root, *root.rglob('*')]:
            found[str(path.relative_to(root.parent))] = stat.S_IMODE(path.lstat().st_mode)
    return found


def check(found, directory_mode, file_mode, label):
    wrong = {p: oct(m) for p, m in found.items()
             if m != (directory_mode if os.path.isdir(os.path.join(base, p)) else file_mode)}
    assert not wrong, (label, wrong)


def session(sql_before_use, work):
    host = Host()
    connection = host.connect()
    try:
        host.query(connection, f"LOAD '{EXTENSION}';" + sql_before_use)
        return work(host, connection)
    finally:
        host.close()


with tempfile.TemporaryDirectory(prefix='differential-shared-') as tmp:
    base = Path(tmp)
    os.chown(base, -1, os.getgid())  # new directories inherit this group (BSD) and keep setgid
    previous = os.umask(0o077)
    try:
        # Private by default, even with a permissive umask.
        os.umask(0o000)
        private, private_pool = base / 'private', base / 'private-pool'

        def private_work(host, c):
            host.query(c, f"ATTACH 'differential:p' AS p (DATA_PATH '{private_pool}'); CREATE TABLE p.t AS SELECT 1 AS v;"
                          "CALL differential_create_snapshot('p'); ATTACH 'differential:p' AS old (SNAPSHOT_VERSION 1);")
            return modes(private, private_pool)
        found = session(f"SET differential_repository='{private}';", private_work)
        assert any('journal/' in p for p in found) and any('pins/' in p for p in found), sorted(found)
        check(found, 0o700, 0o600, 'private repository and pool')
        passed('repositories are private by default: 0700 directories and 0600 files (umask 000)')

        # Shared on request, under a restrictive umask; journal, pins, locks and pool included.
        os.umask(0o077)
        shared, shared_pool = base / 'shared', base / 'shared-pool'

        def shared_work(host, c):
            host.query(c, f"ATTACH 'differential:s' AS s (DATA_PATH '{shared_pool}'); CREATE TABLE s.t AS SELECT 1 AS v;"
                          "CALL differential_create_snapshot('s'); ATTACH 'differential:s' AS old (SNAPSHOT_VERSION 1);")
            assert host.query(c, "SELECT count(*) FROM differential_status() WHERE capability='repository_shared' AND status LIKE 'true%';") == 1
            return modes(shared, shared_pool)
        found = session(f"SET differential_repository_shared=true; SET differential_repository='{shared}';", shared_work)
        assert any('journal/' in p for p in found) and any('pins/' in p for p in found), sorted(found)
        check(found, 0o2770, 0o660, 'shared repository and pool')
        passed('shared repositories: setgid 02770 directories and 0660 files (umask 077), pools included')

        # Users joining a shared repository need no setting; their new files are shared too.
        def join_work(host, c):
            host.query(c, "ATTACH 'differential:s' AS s; INSERT INTO s.t VALUES (2); CALL differential_create_snapshot('s');"
                          "ATTACH 'differential:s' AS old2 (SNAPSHOT_VERSION 2);")
            assert host.query(c, 'SELECT sum(v) FROM s.t;') == 3
            return modes(shared, shared_pool)
        found = session(f"SET differential_repository='{shared}';", join_work)
        check(found, 0o2770, 0o660, 'shared repository after a session without the setting')
        passed('sessions joining a shared repository need no setting and keep its modes')

        # Sharing is chosen at creation: asking for it later is refused.
        def refuse_existing(host, c):
            try:
                host.query(c, "ATTACH 'differential:p' AS p;")
            except RuntimeError as error:
                return str(error)
            return ''
        message = session(f"SET differential_repository_shared=true; SET differential_repository='{private}';", refuse_existing)
        assert 'created for a single user' in message, message

        def refuse_after_use(host, c):
            host.query(c, "ATTACH 'differential:p' AS p;")
            try:
                host.query(c, 'SET differential_repository_shared=true;')
            except RuntimeError as error:
                return str(error)
            return ''
        message = session(f"SET differential_repository='{private}';", refuse_after_use)
        assert 'already open for a single user' in message, message
        check(modes(private, private_pool), 0o700, 0o600, 'private repository after refused requests')
        passed('sharing an existing private repository is refused (before and after first use)')
    finally:
        os.umask(previous)
evidence_path('shared-repository.json').write_text(json.dumps({'passed': results}, indent=2) + '\n')
