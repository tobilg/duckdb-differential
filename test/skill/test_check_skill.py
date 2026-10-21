"""Packaging regressions: malformed metadata and nonportable bundles must fail validation."""
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from check_skill import SKILL_PATH, check, check_frontmatter, check_links


class SkillValidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        shutil.copytree(ROOT / 'plugins/duckdb-differential', self.root / 'plugins/duckdb-differential')
        shutil.copytree(ROOT / '.claude-plugin', self.root / '.claude-plugin')
        shutil.copyfile(ROOT / 'package.json', self.root / 'package.json')
        self.skill = self.root / SKILL_PATH
        self.text = (self.skill / 'SKILL.md').read_text()

    def change_json(self, relative, change):
        path = self.root / relative
        data = json.loads(path.read_text())
        change(data)
        path.write_text(json.dumps(data))

    def test_current_bundle_and_installed_copy(self):
        self.assertEqual(check(self.root), [])
        installed = self.root / '.agents/skills/duckdb-differential'
        shutil.copytree(self.skill, installed)
        self.assertEqual(check_links(installed), [])
        self.assertEqual(check_frontmatter((installed / 'SKILL.md').read_text(), installed.name)[1], [])

    def test_invalid_frontmatter(self):
        for text in ('# no frontmatter', '---\nname: [unterminated\n---\n',
                     '---\n- not a mapping\n---\n',
                     self.text.replace('license: MIT', 'name: another-name\nlicense: MIT'),
                     self.text.replace('license: MIT', 'license: !!python/object:object {}')):
            with self.subTest(text=text[:80]):
                self.assertTrue(check_frontmatter(text, self.skill.name)[1])

    def test_portable_name_requirements(self):
        for name in ('', 'DuckDB', 'duckdb--differential', '-duckdb', 'duckdb-', 'a' * 65, 'other-skill'):
            with self.subTest(name=name):
                text = self.text.replace('name: duckdb-differential', f'name: "{name}"', 1)
                self.assertTrue(check_frontmatter(text, self.skill.name)[1])

    def test_metadata_types_and_limits(self):
        for field, value in (('description', '""'), ('description', 'true'),
                             ('description', 'x' * 1025), ('compatibility', 'x' * 501),
                             ('metadata', '{version: 1}')):
            with self.subTest(field=field, value=value[:20]):
                text = f'---\nname: duckdb-differential\n'
                if field != 'description':
                    text += 'description: Version DuckDB databases\n'
                text += f'{field}: {value}\n---\n'
                self.assertTrue(check_frontmatter(text, self.skill.name)[1])

    def test_missing_bundled_reference(self):
        (self.skill / 'references/api.md').unlink()
        self.assertTrue(check(self.root))

    def test_link_cannot_depend_on_files_outside_installed_skill(self):
        (self.root / 'outside.md').write_text('Not shipped with the skill.')
        with (self.skill / 'SKILL.md').open('a') as file:
            file.write('\n[Outside](../../../../outside.md)\n')
        self.assertTrue(check(self.root))

    def test_remote_urls_anchors_and_examples_do_not_require_files(self):
        with (self.skill / 'SKILL.md').open('a') as file:
            file.write('\n[Web](https://example.invalid/doc.md) [Section](#setup)\n'
                       '`[Example](missing.md)`\n```markdown\n[Example](missing.md)\n```\n')
        self.assertEqual(check(self.root), [])

    def test_missing_pi_skill_path(self):
        self.change_json('package.json', lambda data: data['pi'].update(skills=['./missing']))
        self.assertTrue(check(self.root))

    def test_duplicate_pi_discovery(self):
        self.change_json('package.json', lambda data: data['pi']['skills'].extend(data['pi']['skills']))
        self.assertTrue(check(self.root))

    def test_misdirected_claude_marketplace(self):
        self.change_json('.claude-plugin/marketplace.json',
                         lambda data: data['plugins'][0].update(source='./missing'))
        self.assertTrue(check(self.root))

    def test_package_version_drift(self):
        self.change_json('package.json', lambda data: data.update(version='99.0.0'))
        self.assertTrue(check(self.root))

    def test_invalid_package_json(self):
        for text in ('{', '[]', 'null', '{"pi": {"skills": "not a list"}}'):
            with self.subTest(text=text):
                (self.root / 'package.json').write_text(text)
                self.assertTrue(check(self.root))

    def test_invalid_claude_plugin_shape(self):
        (self.root / 'plugins/duckdb-differential/.claude-plugin/plugin.json').write_text('[]')
        self.assertTrue(check(self.root))


if __name__ == '__main__':
    unittest.main()
