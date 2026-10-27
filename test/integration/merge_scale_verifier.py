#!/usr/bin/env python3
"""Adversarial checks of the scale workload's exact native-value/catalog oracle."""
from pathlib import Path
import shutil
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from merge_qualification_common import QualificationError, require, literal
from merge_scale_common import DDL, ScaleFixture, expected_values
from merge_native_spill import arguments


def main():
    args=arguments().parse_args([])
    with tempfile.TemporaryDirectory(prefix='differential-scale-verifier-') as tmp:
        root=Path(tmp); f=ScaleFixture(root,args,rows=1000,percent=20)
        baseline=root/'baseline.duckdb'
        insert=(f"INSERT INTO x SELECT {','.join(expected_values('i',200))} "
                'FROM range(1002) r(i) WHERE i NOT IN (998,999);')
        f.run(f'ATTACH {literal(baseline)} AS d; USE d;'+DDL+insert)
        f.verify_tables(f.cli,f'ATTACH {literal(baseline)} AS actual (READ_ONLY);')
        cases=[
            ('null_array','UPDATE x SET fixed=NULL WHERE id=1;','typed_result_mismatch'),
            ('struct_child','UPDATE x SET attrs=struct_update(attrs,n:=0::UHUGEINT) WHERE id=1;','typed_result_mismatch'),
            ('list_of_array','UPDATE x SET arrays=[[1,2]::DOUBLE[2]] WHERE id=1;','typed_result_mismatch'),
            ('timestamp','UPDATE x SET ts=make_timestamp_ns(1577836800123456799)::TIMESTAMPTZ_NS WHERE id=1;','typed_result_mismatch'),
            ('missing_key','DELETE FROM x WHERE id=0;','typed_result_mismatch'),
            ('wrong_key_domain','DELETE FROM x WHERE id=0; INSERT INTO x SELECT '+','.join(expected_values('1002',200))+';','typed_result_mismatch'),
            ('missing_index','DROP INDEX by_score;','catalog_mismatch'),
            ('duplicate_without_pk','DELETE FROM x WHERE id=0; INSERT INTO x SELECT * FROM x WHERE id=1;','catalog_mismatch'),
        ]
        for name,mutation,code in cases:
            actual=root/(name+'.duckdb')
            if name=='duplicate_without_pk':
                # Every row individually matches its key, with the right total
                # count. The missing native PK must still invalidate the proof.
                f.run(f'ATTACH {literal(actual)} AS d; USE d;'+DDL.replace(' PRIMARY KEY','')+insert)
            else: shutil.copyfile(baseline,actual)
            f.run(f'ATTACH {literal(actual)} AS d; USE d;'+mutation)
            try: f.verify_tables(f.cli,f'ATTACH {literal(actual)} AS actual (READ_ONLY);')
            except QualificationError as error: require(error.code==code,'wrong_oracle_rejection')
            else: raise QualificationError('invalid_scale_result_accepted')
            print('PASS scale verifier rejects '+name,flush=True)
        f.cli.check_artifacts(f.identity)


if __name__=='__main__': main()
