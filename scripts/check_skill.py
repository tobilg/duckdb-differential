#!/usr/bin/env python3
"""Statically validate our portable skill and manifests. Never import or run a harness loader."""
import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

try:
    import yaml
except ImportError:
    sys.exit('error: PyYAML is missing; run: python3 -m pip install -r scripts/requirements-skill.txt '
             '(in a virtual environment, see README.md#development)')

ROOT = Path(__file__).resolve().parent.parent
SKILL_PATH = Path('plugins/duckdb-differential/skills/duckdb-differential')


class UniqueKeyLoader(yaml.SafeLoader):
    """Reject ambiguous YAML that different agent loaders could interpret differently."""


def unique_mapping(loader, node):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node)
        if not isinstance(key, str):
            raise ValueError('YAML mapping keys must be strings')
        if key in mapping:
            raise ValueError(f'duplicate YAML key: {key}')
        mapping[key] = loader.construct_object(value_node)
    return mapping


UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)


def check_frontmatter(text, directory_name):
    match = re.match(r'\A---\n(.*?)\n---(?:\n|$)', text, re.S)
    if not match:
        return {}, ['SKILL.md requires YAML frontmatter delimited by ---']
    try:
        fields = yaml.load(match[1], Loader=UniqueKeyLoader)
    except (yaml.YAMLError, ValueError) as error:
        return {}, [f'invalid SKILL.md frontmatter: {error}']
    if not isinstance(fields, dict):
        return {}, ['SKILL.md frontmatter must be a mapping']
    problems = []
    name = fields.get('name')
    if not isinstance(name, str) or len(name) > 64 or not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', name):
        problems.append('skill name must be 1-64 lowercase letters, digits and single hyphens')
    elif name != directory_name:
        problems.append('skill name must match its directory name')
    for field, limit in (('description', 1024), ('compatibility', 500)):
        if field == 'compatibility' and field not in fields:
            continue
        value = fields.get(field)
        if not isinstance(value, str) or not value.strip() or len(value) > limit:
            problems.append(f'{field} must be a nonempty string of at most {limit} characters')
    for field in ('license', 'allowed-tools'):
        if field in fields and not isinstance(fields[field], str):
            problems.append(f'{field} must be a string')
    metadata = fields.get('metadata', {})
    if not isinstance(metadata, dict) or not all(isinstance(v, str) for v in metadata.values()):
        problems.append('metadata must map string keys to string values')
    return fields, problems


def check_links(skill):
    problems = []
    for document in sorted(skill.rglob('*.md')):
        # The bundled docs use inline Markdown links. Check file targets, not URL availability
        # or heading anchors. Ignore fenced examples and inline code.
        text = re.sub(r'^```[^\n]*\n.*?^```[^\n]*$', '', document.read_text(), flags=re.M | re.S)
        text = re.sub(r'`[^`\n]*`', '', text)
        for match in re.finditer(r'\]\(<?([^\s)>]+)>?(?:\s+"[^"]*")?\)', text):
            try:
                target = urlsplit(match[1])
            except ValueError:
                problems.append(f'{document.relative_to(skill)}: invalid link target: {match[1]}')
                continue
            if target.scheme or target.netloc or not target.path:
                continue
            path = (document.parent / unquote(target.path)).resolve()
            if not path.is_relative_to(skill.resolve()) or not path.is_file():
                problems.append(f'{document.relative_to(skill)}: local link is not a bundled file: {match[1]}')
    return problems


def check_packages(root, skill, fields):
    problems = []
    try:
        package = json.loads((root / 'package.json').read_text())
        marketplace = json.loads((root / '.claude-plugin/marketplace.json').read_text())
        plugin_root = root / 'plugins/duckdb-differential'
        plugin = json.loads((plugin_root / '.claude-plugin/plugin.json').read_text())
        if not all(isinstance(value, dict) for value in (package, marketplace, plugin)):
            raise ValueError('package and marketplace manifests must be JSON objects')
        paths = package['pi']['skills']
        if not isinstance(paths, list) or not paths or not all(isinstance(p, str) for p in paths):
            raise ValueError('pi.skills must be a nonempty list of paths')
        # This package deliberately uses literal directories, so installs expose one canonical
        # skill. A new layout should update these checks alongside its manifests.
        discovered = []
        for entry in paths:
            directory = (root / entry).resolve()
            if not directory.is_relative_to(root.resolve()) or not directory.is_dir():
                problems.append(f'pi skill directory does not exist inside the repository: {entry}')
                continue
            discovered.extend(p.resolve() for p in directory.rglob('SKILL.md'))
        if discovered != [(skill / 'SKILL.md').resolve()]:
            problems.append('pi.skills must discover exactly the canonical SKILL.md')
        entries = marketplace['plugins']
        if not isinstance(entries, list) or not all(isinstance(p, dict) for p in entries):
            raise ValueError('Claude marketplace plugins must be a list of objects')
        entries = [p for p in entries if p.get('name') == skill.name]
        if len(entries) != 1 or (root / entries[0]['source']).resolve() != plugin_root.resolve():
            problems.append('Claude marketplace must resolve to the canonical plugin directory')
        if plugin.get('name') != skill.name:
            problems.append('Claude plugin name must match the skill name')
        metadata = fields.get('metadata', {})
        version = metadata.get('version') if isinstance(metadata, dict) else None
        if not isinstance(version, str) or package.get('version') != version or plugin.get('version') != version:
            problems.append('pi package, Claude plugin and skill metadata versions must match')
    except (OSError, ValueError, KeyError, TypeError) as error:
        problems.append(f'invalid skill package metadata: {error}')
    return problems


def check(root):
    skill = root / SKILL_PATH
    try:
        fields, problems = check_frontmatter((skill / 'SKILL.md').read_text(), skill.name)
        return problems + check_links(skill) + check_packages(root, skill, fields)
    except OSError as error:
        return [str(error)]


def main():
    problems = check(ROOT)
    for problem in problems:
        print(f'error: {problem}', file=sys.stderr)
    if not problems:
        print('Skill metadata, bundled links and Claude/pi package paths are valid.')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
