package filters_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/filters"
	"github.com/stretchr/testify/assert"
)

func TestExtractCaseSensitivity(t *testing.T) {
	caseTests := []struct {
		q           string // input query
		wantQ       string // query after extraction
		wantEnabled bool   // q is case sensitive
	}{
		{"case:n foo", " foo", false},
		{"foo case:y", "foo ", true},
		{"foo bar", "foo bar", false},
	}
	for _, ct := range caseTests {
		gotQ, gotEnabled := filters.ExtractCaseSensitivity(ct.q)
		assert.Equal(t, ct.wantQ, gotQ, "extracted query should match")
		assert.Equal(t, ct.wantEnabled, gotEnabled, "case sensitivity was not detected correctly")
	}
}

func TestExtractFilenameFilter(t *testing.T) {
	filenameTests := []struct {
		q            string // input query
		wantQ        string // query after extraction
		wantFilename string // extracted filename
	}{
		{"file:bar.zip foo", " foo", "bar.zip"},
		{"a b c f:/foo/bar.zip", "a b c ", "/foo/bar.zip"},
		{"a b f f:a/b/bar-file.cpp", "a b f ", "a/b/bar-file.cpp"},
	}
	for _, ft := range filenameTests {
		gotQ, gotFilename := filters.ExtractFilenameFilter(ft.q)
		assert.Equal(t, ft.wantQ, gotQ, "extracted query should match")
		assert.Equal(t, ft.wantFilename, gotFilename, "extracted filename mismatch")
	}
}

func TestExtractLanguageFilter(t *testing.T) {
	langTests := []struct {
		q        string // input query
		wantQ    string // query after extraction
		wantLang string // extracted language
	}{
		{"lang:java System.out.println", " System.out.println", "java"},
		{"fprintf lang:cpp", "fprintf ", "c++"},
		{"any old lang", "any old lang", ""},
	}
	for _, lt := range langTests {
		gotQ, gotLang := filters.ExtractLanguageFilter(lt.q)
		assert.Equal(t, lt.wantQ, gotQ, "extracted query should match")
		assert.Equal(t, lt.wantLang, gotLang, "extracted lang mismatch")
	}
}

func TestExtractRepoFilter(t *testing.T) {
	repoTests := []struct {
		q        string // input query
		wantQ    string // query after extraction
		wantRepo string // extracted language
	}{
		{"repo:buildbuddy-io buildbuddy", " buildbuddy", "buildbuddy-io"},
		{"REPO repo:buildbuddy-internal", "REPO ", "buildbuddy-internal"},
		{"repo:bazel toolchain", " toolchain", "bazel"},
	}
	for _, rt := range repoTests {
		gotQ, gotRepo := filters.ExtractRepoFilter(rt.q)
		assert.Equal(t, rt.wantQ, gotQ, "extracted query should match")
		assert.Equal(t, rt.wantRepo, gotRepo, "extracted repo mismatch")
	}
}
