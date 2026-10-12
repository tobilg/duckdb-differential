"""Evidence output: test and build scripts write their result transcripts to build/evidence/
(or DIFFERENTIAL_EVIDENCE_DIR). Transcripts are local artifacts and are not tracked."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def evidence_path(name):
    folder = Path(os.environ.get('DIFFERENTIAL_EVIDENCE_DIR', str(ROOT / 'build/evidence')))
    folder.mkdir(parents=True, exist_ok=True)
    return folder / name
