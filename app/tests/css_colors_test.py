#!/usr/bin/env python3
"""Tests for CSS color variable usage."""

import os
import unittest

RUNFILES = os.environ.get('RUNFILES_DIR', '')
COLORS_CSS = os.path.join(RUNFILES, '_main/app/root/colors.css')
STYLE_CSS = os.path.join(RUNFILES, '_main/enterprise/app/style.css')


def parse_definitions(css):
    """Parse --name: value; definitions from CSS."""
    defs = set()
    for line in css.splitlines():
        line = line.strip()
        if line.startswith('--') and ':' in line:
            name = line.split(':')[0].strip().lstrip('-')
            defs.add(name)
    return defs


def parse_references(css):
    """Parse var(--name) references from CSS."""
    refs = set()
    for line in css.splitlines():
        i = 0
        while True:
            start = line.find('var(--', i)
            if start == -1:
                break
            start += 6  # len('var(--')
            end = start
            while end < len(line) and (line[end].isalnum() or line[end] == '-'):
                end += 1
            if end > start:
                refs.add(line[start:end])
            i = end
    return refs


class CSSVariablesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open(COLORS_CSS) as f:
            cls.colors_css = f.read()
        with open(STYLE_CSS) as f:
            cls.style_css = f.read()
        cls.defined = parse_definitions(cls.colors_css)

    def test_no_unused_color_variables(self):
        """All color-* variables in colors.css should be used."""
        used = parse_references(self.style_css) | parse_references(self.colors_css)
        defined_colors = {v for v in self.defined if v.startswith('color-')}
        unused = defined_colors - used

        if unused:
            self.fail(f"Unused color variables:\n" +
                      "\n".join(f"  --{v}" for v in sorted(unused)))

    def test_no_undefined_color_variables(self):
        """All color-* references should be defined in colors.css."""
        used = parse_references(self.style_css)
        used_colors = {v for v in used if v.startswith('color-')}
        undefined = used_colors - self.defined

        if undefined:
            self.fail(f"Undefined color variables:\n" +
                      "\n".join(f"  --{v}" for v in sorted(undefined)))


if __name__ == '__main__':
    unittest.main()
