# Test Health JUnit parser

The parser converts one bounded JUnit XML document into case records. The case name is the testcase `name` exactly as reported; `classname` is not part of Test Health identity.

`failure` and `error` map to `FAIL`; a timeout status maps to `TIMEOUT`; skipped or disabled tests map to `UNKNOWN`. Failure element bodies and system output are ignored. Only the bounded `message` attribute is retained.
