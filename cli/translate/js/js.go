package js

import (
	_ "embed"
	"fmt"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/translate/builtins"
	"github.com/dop251/goja"
)

//go:embed translate.js
var translateScript string

var (
	// Pattern to extract import statements of the form:
	// import { foo, bar } from "//path/to/target"
	pattern        = `import\s*{\s*([\w\s,"]+?)\s*}\s*from\s*"([^"]+)";`
	replacePattern = `load("$2", "$1");`
	regex          = regexp.MustCompile(pattern)

	// Pattern to properly quote comma separated load statements
	commaPattern        = `\s*,\s*`
	commaReplacePattern = `"$0"`
	commaRegex          = regexp.MustCompile(commaPattern)
)

type jsTranslator struct{}

func New() *jsTranslator {
	return &jsTranslator{}
}

func (j *jsTranslator) Translate(name, input string) (string, error) {
	input = j.translateImports(input)

	vm := goja.New()

	globals := fmt.Sprintf(`let globals = ["%s"]; let rules = ["%s"];`,
		strings.Join(builtins.BuildGlobals, `", "`),
		strings.Join(builtins.Rules, `", "`))

	v, err := vm.RunScript("globals.js", globals)
	if err != nil {
		return "", err
	}

	v, err = vm.RunScript("translate.js", translateScript)
	if err != nil {
		return "", err
	}

	v, err = vm.RunScript(name, input)
	if err != nil {
		return "", err
	}
	v, err = vm.RunScript("output.js", "output")
	if err != nil {
		return "", err
	}
	return v.String(), nil
}

func (j *jsTranslator) translateImports(input string) string {
	// Replace import statements with load statements
	return regex.ReplaceAllStringFunc(input, func(match string) string {
		match = commaRegex.ReplaceAllString(match, commaReplacePattern)
		return regex.ReplaceAllString(match, replacePattern)
	})
}
