#!/usr/bin/env python3
"""Extended native equivalence with non-default allocation sizes and historical roots."""
from pathlib import Path
import json
import os
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

EXT=EXTENSION
CLI=DUCKDB_CLI

def run(sql,folder):
    env=dict(os.environ,DIFFERENTIAL_ENABLE_PROBES='1',DIFFERENTIAL_STORE_ROOT=str(folder),DIFFERENTIAL_PROBE_CAPTURE='1')
    r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-csv'],input=f"LOAD '{EXT}';\n"+sql,
                     text=True,capture_output=True,env=env,timeout=60)
    if r.returncode:raise AssertionError((r.returncode,r.stdout,r.stderr,sql))
    return r.stdout

fixture="""
USE d;
CREATE SCHEMA extra;
CREATE TYPE extra.mood AS ENUM('happy','sad');
CREATE MACRO extra.twice(x) AS x*2;
CREATE SEQUENCE extra.seq START 4;
CREATE TABLE extra.items(id INTEGER PRIMARY KEY DEFAULT nextval('extra.seq'), mood extra.mood NOT NULL, amount INT CHECK(amount>=0));
INSERT INTO extra.items(mood,amount) VALUES('happy',10),('sad',20);
ALTER TABLE extra.items RENAME COLUMN amount TO value;
ALTER TABLE extra.items RENAME TO renamed;
CREATE VIEW extra.v AS SELECT id,extra.twice(value) AS doubled FROM extra.renamed;
CREATE INDEX mood_idx ON extra.renamed(mood);
CREATE TABLE extra.bulk AS SELECT range AS id, sha256(range::VARCHAR) AS payload FROM range(20000);
BEGIN; DELETE FROM extra.bulk WHERE id<100; CREATE TABLE extra.rolled_back AS SELECT 1; ROLLBACK;
CREATE TABLE extra.recreated AS SELECT 0 AS n;
DROP TABLE extra.recreated;
CREATE TABLE extra.recreated AS SELECT 1 AS n;
CHECKPOINT d;
"""
report="""
SELECT * FROM extra.renamed ORDER BY id;
SELECT * FROM extra.v ORDER BY id;
SELECT count(*),sum(id),min(payload),max(payload) FROM extra.bulk;
SELECT * FROM extra.recreated;
SELECT table_schema,table_name,column_name,data_type FROM information_schema.columns WHERE table_catalog='d' ORDER BY ALL;
"""
results=[]
with tempfile.TemporaryDirectory(prefix='differential-matrix-') as tmp:
    folder=Path(tmp)
    for block in (16384,65536,262144):
        case=folder/str(block);case.mkdir()
        native=case/'native.db'
        native_attach=f"ATTACH '{native}' AS d (BLOCK_SIZE {block});"
        diff_attach=f"ATTACH 'diff://probe' AS d (TYPE differential, BLOCK_SIZE {block});"
        expected=run(native_attach+fixture+report,case)
        actual=run(diff_attach+fixture+report,case)
        assert expected==actual,(expected,actual)
        expected=run(native_attach+'USE d;'+report,case)
        assert expected==run(diff_attach+'USE d;'+report,case)
        capture=run(diff_attach+"CALL differential_probe_checkpoint('d');",case)
        root=capture.splitlines()[1].split(',')[1]
        copied=case/'copy.csv'
        # COPY and block reuse after delete/drop/checkpoint exercise native free space.
        changes=f"""USE d; COPY extra.bulk TO '{copied}' (HEADER); DELETE FROM extra.bulk;
COPY extra.bulk FROM '{copied}' (HEADER); DROP INDEX extra.mood_idx;
DROP VIEW extra.v; DROP TABLE extra.renamed; DROP SEQUENCE extra.seq;
ALTER TABLE extra.bulk ADD COLUMN version INT DEFAULT 2;
UPDATE extra.bulk SET payload='changed' WHERE id%2=0;
CHECKPOINT d;"""
        run(diff_attach+changes,case)
        historical=run(f"ATTACH 'diff://roots/{root}' AS d (TYPE differential,READ_ONLY); USE d;"+report,case)
        assert historical==expected,(block,historical,expected)
        results.append({'allocation_size':block,'status':'passed','historical_root':root})
        print('PASS allocation size',block,'extended DDL/DML, COPY, rollback, drop/recreate, fresh-process historical schema/data')
evidence_path('native-matrix-results.json').write_text(json.dumps(results,indent=2)+'\n')
