"""Checks the evidence boundary: a failed command is not necessarily a finding."""
import json
import unittest

import run


def events(*pairs):
    return "\n".join(json.dumps(dict(Test=test, Action=action)) for test, action in pairs)


class EvidenceTest(unittest.TestCase):
    def test_build_failure_is_not_a_finding(self):
        result = run.interpret("compiler: cannot import dependency", 1)
        self.assertEqual("infrastructure_error", result["status"])
        self.assertFalse(result["failures"])

    def test_timeout_is_not_a_validated_finding(self):
        result = run.interpret(events(("TestContract/case=1", "fail")), 124)
        self.assertEqual("infrastructure_error", result["status"])

    def test_interrupted_suite_with_earlier_failure_is_infrastructure(self):
        raw = events(("TestContract", "run"), ("TestContract/a", "run"),
                     ("TestContract/a", "fail"), ("TestContract/b", "run"))
        self.assertEqual("infrastructure_error", run.interpret(raw, 1)["status"])

    def test_absent_or_skipped_replay_is_not_a_pass(self):
        for raw in ("", events(("TestContract", "skip")), events(("TestOther", "pass"))):
            with self.subTest(raw=raw):
                self.assertEqual("infrastructure_error", run.interpret(raw, 0, "TestContract")["status"])

    def test_only_same_scenario_fixed_counts(self):
        before = run.interpret(events(("TestContract/a", "fail"), ("TestContract/b", "pass"), ("TestContract", "fail")), 1)
        after = run.interpret(events(("TestContract/a", "fail"), ("TestContract/b", "pass"), ("TestContract", "fail")), 1)
        self.assertEqual(("missed", []), run.distinguish(before, after))
        self.assertNotIn("TestContract", before["failures"])

    def test_fixed_witness_can_coexist_with_unrelated_failure(self):
        before = run.interpret(events(("TestContract/a", "fail"), ("TestContract/b", "fail")), 1)
        after = run.interpret(events(("TestContract/a", "pass"), ("TestContract/b", "fail")), 1)
        self.assertEqual(("detected", ["TestContract/a"]), run.distinguish(before, after))

    def test_skip_cannot_validate_a_fix(self):
        before = run.interpret(events(("TestContract/a", "fail"), ("TestOther", "pass")), 1)
        after = run.interpret(events(("TestContract/a", "skip"), ("TestOther", "pass")), 0)
        self.assertEqual(("missed", []), run.distinguish(before, after))


if __name__ == "__main__":
    unittest.main()
