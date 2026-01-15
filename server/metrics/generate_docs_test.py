#!/usr/bin/env python3

import unittest
from server.metrics.generate_docs import DocsGenerator


class TestDocsGenerator(unittest.TestCase):
    def test_parse_label_constant_with_single_space(self):
        """Test parsing label constant with single space around ="""
        gen = DocsGenerator("/dev/null")
        gen.state = "LABEL_CONSTANTS"
        gen.process_line(0, '    StatusLabel = "status"')
        self.assertIn("StatusLabel", gen.label_constants)
        self.assertEqual(gen.label_constants["StatusLabel"]["value"], "status")

    def test_parse_label_constant_with_multiple_spaces(self):
        """Test parsing label constant with gofmt-aligned multiple spaces"""
        gen = DocsGenerator("/dev/null")
        gen.state = "LABEL_CONSTANTS"
        gen.process_line(0, '    StatusHumanReadableLabel         = "status"')
        self.assertIn("StatusHumanReadableLabel", gen.label_constants)
        self.assertEqual(gen.label_constants["StatusHumanReadableLabel"]["value"], "status")

if __name__ == "__main__":
    unittest.main()
