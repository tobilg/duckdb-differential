#!/usr/bin/env python3
"""Keep the agent skill (plugins/duckdb-differential/skills/duckdb-differential) in sync with the docs.

Copies the user docs into the skill's references/ (links to docs that are not copied point to GitHub)
and checks that SKILL.md names every function of docs/sql-reference.md. With --check, changes
nothing and fails when the skill is out of date: python3 scripts/sync_skill.py [--check]"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKILL = ROOT / 'plugins' / 'duckdb-differential' / 'skills' / 'duckdb-differential'
REFERENCES = ('sql-reference.md', 'lifecycle.md', 'shared-databases.md', 'api.md', 'deployment.md')
DOCS_URL = 'https://github.com/tobilg/duckdb-differential/blob/main/docs/'
HEADER = '<!-- Generated from docs/{name} by scripts/sync_skill.py; edit the original. -->\n\n'


def reference(name):
    text = (ROOT / 'docs' / name).read_text()

    def link(match):
        target = match.group(1)
        return match.group(0) if target.split('#')[0] in REFERENCES else f']({DOCS_URL}{target})'

    return HEADER.format(name=name) + re.sub(r'\]\(([a-z0-9-]+\.md(?:#[^)]*)?)\)', link, text)


def main():
    check = sys.argv[1:] == ['--check']
    if sys.argv[1:] and not check:
        print('usage: sync_skill.py [--check]', file=sys.stderr)
        return 2
    problems = []
    for name in REFERENCES:
        path = SKILL / 'references' / name
        content = reference(name)
        if path.exists() and path.read_text() == content:
            continue
        if check:
            problems.append(f'{path.relative_to(ROOT)} differs from docs/{name}')
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            print(f'updated {path.relative_to(ROOT)}')
    skill = (SKILL / 'SKILL.md').read_text()
    functions = re.findall(r'^### (differential_[a-z_]+)$', (ROOT / 'docs' / 'sql-reference.md').read_text(), re.M)
    problems += [f'SKILL.md does not name {f}' for f in functions if f'`{f}`' not in skill]
    for problem in problems:
        print(f'error: {problem}', file=sys.stderr)
    if problems and check:
        print('run: python3 scripts/sync_skill.py', file=sys.stderr)
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
