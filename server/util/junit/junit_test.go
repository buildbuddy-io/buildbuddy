package junit

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNestedSuitesAndRepeatedFailures(t *testing.T) {
	xml := `<testsuites><testsuite name="outer"><testsuite name="inner">` +
		`<testcase name="test" classname="pkg.C">` +
		`<failure type="AssertionError" message="want 1">first body</failure>` +
		`<error type="Panic" message="boom"><![CDATA[second <body>]]></error>` +
		`</testcase></testsuite></testsuite></testsuites>`

	cases, err := Parse(strings.NewReader(xml), DefaultLimits())

	require.NoError(t, err)
	require.Equal(t, []TestCase{{
		SuiteName: "inner",
		ClassName: "pkg.C",
		Name:      "test",
		Failures: []Failure{
			{Kind: "failure", Type: "AssertionError", Message: "want 1", Body: "first body"},
			{Kind: "error", Type: "Panic", Message: "boom", Body: "second <body>"},
		},
	}}, cases)
}

func TestParseNamespaces(t *testing.T) {
	xml := `<j:testsuite xmlns:j="urn:junit" name="suite"><j:testcase name="test" classname="C">` +
		`<j:failure message="bad">body</j:failure></j:testcase></j:testsuite>`

	cases, err := Parse(strings.NewReader(xml), DefaultLimits())

	require.NoError(t, err)
	require.Len(t, cases, 1)
	require.Equal(t, "suite", cases[0].SuiteName)
	require.Equal(t, "bad", cases[0].Failures[0].Message)
}

func TestParseMissingFieldsAndEncodingDeclaration(t *testing.T) {
	xml := `<?xml version="1.0" encoding="ISO-8859-1"?>` +
		`<testsuite><testcase><failure/></testcase></testsuite>`

	cases, err := Parse(strings.NewReader(xml), DefaultLimits())

	require.NoError(t, err)
	require.Equal(t, []TestCase{{Failures: []Failure{{Kind: "failure"}}}}, cases)
}

func TestParseManyPassingCasesDoesNotExhaustFailedCaseLimit(t *testing.T) {
	var xml strings.Builder
	xml.WriteString(`<testsuite>`)
	for i := 0; i < 101; i++ {
		xml.WriteString(`<testcase name="passing"/>`)
	}
	xml.WriteString(`<testcase name="failed"><failure message="boom"/></testcase></testsuite>`)

	cases, err := Parse(strings.NewReader(xml.String()), DefaultLimits())

	require.NoError(t, err)
	require.Len(t, cases, 1)
	require.Equal(t, "failed", cases[0].Name)
}

func TestParseSanitizesInvalidUTF8AndIllegalXMLCharacters(t *testing.T) {
	data := append([]byte(`<testsuite><testcase name="t"><failure message="`), 0xff, 0x00)
	data = append(data, []byte(`">a`)...)
	data = append(data, 0x1b)
	data = append(data, []byte(`b</failure></testcase></testsuite>`)...)

	cases, err := Parse(strings.NewReader(string(data)), DefaultLimits())

	require.NoError(t, err)
	require.Equal(t, "�", cases[0].Failures[0].Message)
	require.Equal(t, "ab", cases[0].Failures[0].Body)
}

func TestParseRejectsMalformedXMLAndDirectives(t *testing.T) {
	for _, tc := range []struct {
		name string
		xml  string
	}{
		{name: "malformed", xml: `<testsuite><testcase></testsuite>`},
		{name: "directive", xml: `<!DOCTYPE testsuite [<!ENTITY x SYSTEM "file:///etc/passwd">]><testsuite/>`},
		{name: "processing instruction", xml: `<?run command?><testsuite/>`},
		{name: "unknown entity", xml: `<testsuite name="&external;"/>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tc.xml), DefaultLimits())
			require.Error(t, err)
		})
	}
}

func TestParseTolerantModeStillRejectsDirectives(t *testing.T) {
	limits := DefaultLimits()
	limits.TolerateMalformedXML = true
	cases, err := Parse(strings.NewReader(`<testsuite><testcase name="legacy"><failure message="one & two"/></testcase></testsuite>`), limits)
	require.NoError(t, err)
	require.Len(t, cases, 1)
	require.Equal(t, "one & two", cases[0].Failures[0].Message)

	_, err = Parse(strings.NewReader(`<!DOCTYPE testsuite><testsuite/>`), limits)
	require.Error(t, err)
}

func TestParseLimits(t *testing.T) {
	base := DefaultLimits()
	tests := []struct {
		name   string
		xml    string
		adjust func(*Limits)
	}{
		{name: "input", xml: `<testsuite/>`, adjust: func(l *Limits) { l.MaxInputBytes = 5 }},
		{name: "depth", xml: `<a><b><c/></b></a>`, adjust: func(l *Limits) { l.MaxDepth = 2 }},
		{name: "tokens", xml: `<a><b/></a>`, adjust: func(l *Limits) { l.MaxTokens = 2 }},
		{name: "failed testcases", xml: `<testsuite><testcase><failure/></testcase><testcase><failure/></testcase></testsuite>`, adjust: func(l *Limits) { l.MaxFailedTestCases = 1; l.MaxFailures = 2 }},
		{name: "failures", xml: `<testsuite><testcase><failure/><error/></testcase></testsuite>`, adjust: func(l *Limits) { l.MaxFailures = 1 }},
		{name: "attribute text", xml: `<testsuite><testcase name="long"/></testsuite>`, adjust: func(l *Limits) { l.MaxFieldBytes = 3 }},
		{name: "body text", xml: `<testsuite><testcase><failure>long</failure></testcase></testsuite>`, adjust: func(l *Limits) { l.MaxFieldBytes = 3 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := base
			tc.adjust(&limits)
			_, err := Parse(strings.NewReader(tc.xml), limits)
			require.ErrorContains(t, err, "limit exceeded")
		})
	}
}

func TestParseReturnsBoundedPartialResults(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxFailedTestCases = 1
	cases, err := Parse(strings.NewReader(`<testsuite><testcase name="first"><failure/></testcase><testcase name="second"><failure/></testcase></testsuite>`), limits)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrResultLimit))
	require.Len(t, cases, 1)
	require.Equal(t, "first", cases[0].Name)
}

func TestParseRequiresPositiveLimits(t *testing.T) {
	_, err := Parse(strings.NewReader(`<testsuite/>`), Limits{})
	require.ErrorContains(t, err, "all limits must be positive")
}
