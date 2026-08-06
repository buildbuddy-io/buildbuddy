# TestBuddy JUnit parser

The parser converts one bounded JUnit XML document into case records. The case name is the testcase `name` exactly as reported; `classname` is not part of TestBuddy identity.

`failure` and `error` map to `FAIL`; a timeout status maps to `TIMEOUT`; skipped or disabled tests map to `UNKNOWN`. The bounded failure message combines the element's `message` attribute and body. System output is ignored.
