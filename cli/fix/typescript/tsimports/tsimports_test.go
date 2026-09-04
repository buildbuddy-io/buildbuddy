package tsimports

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImports(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  string
		jsx  bool
		want []string
	}{
		{"default", `import React from "react";`, false, []string{"react"}},
		{"named", `import { a, b as c } from './x'`, false, []string{"./x"}},
		{"namespace", `import * as fs from "fs"`, false, []string{"fs"}},
		{"default and named", "import a, { b } from 'm'\nimport d from \"n\"", false, []string{"m", "n"}},
		{"type import", `import type { T } from "./types"`, false, []string{"./types"}},
		{"default named type", `import type from "./type-module"`, false, []string{"./type-module"}},
		{"multiline", "import {\n  a,\n  b,\n} from\n  \"multi\";", false, []string{"multi"}},
		{"import attributes", `import data from "./data.json" with { type: "json" };`, false, []string{"./data.json"}},
		{"string module name", `import { "a-b" as ab } from "mod"`, false, []string{"mod"}},
		{"side effect import", `import "./polyfill"; import x from "y"`, false, []string{"./polyfill", "y"}},
		{"import require", `import fs = require("fs"); import type t = require('./t'); import x from "y"`, false, []string{"fs", "./t", "y"}},
		{"namespace alias ignored", `import A = N.B; import x from "y"`, false, []string{"y"}},
		{"dynamic import ignored", `const m = import("dyn"); import x from "y"`, false, []string{"y"}},
		{"import.meta", `const u = import.meta.url; import x from "y"`, false, []string{"y"}},
		{"export from", `export * from "a"; export * as ns from "a2"; export { b, c as d } from "b"; export type { T } from "t"; export { e }; export const f = 1; export default g; import x from "y"`, false, []string{"a", "a2", "b", "t", "y"}},
		{"line comment", "// import fake from \"fake\"\nimport x from \"y\"", false, []string{"y"}},
		{"block comment", "/* import fake from \"fake\" */ import x from \"y\"", false, []string{"y"}},
		{"in string", `const s = 'import fake from "fake"'; import x from "y"`, false, []string{"y"}},
		{"in template", "const s = `import fake from \"fake\" ${'`'} ${`nested ${1}`}`; import x from \"y\"", false, []string{"y"}},
		{"in regexp", `const r = /import fake from "fake"/g; import x from "y"`, false, []string{"y"}},
		{"regexp with quote", `const r = /"/; const q = /'/; import x from "y"`, false, []string{"y"}},
		{"regexp class with slash", `const r = /[/]'/; import x from "y"`, false, []string{"y"}},
		{"division not regexp", `const a = b / c; const d = "q"; import x from "y"`, false, []string{"y"}},
		{"division after paren", `const a = (b) / 2 / 3; import x from "y"`, false, []string{"y"}},
		{"regexp after return", "function f() { return /'/.test(x) }\nimport x from \"y\"", false, []string{"y"}},
		{"nested in block ignored", `declare module "m" { import z from "z"; } import x from "y"`, false, []string{"y"}},
		{"nested in function ignored", `function f() { import("a"); } import x from "y"`, false, []string{"y"}},
		{"property named import", `foo.import("a"); obj.import; import x from "y"`, false, []string{"y"}},
		{"no semicolons", "const a = 1\nimport x from \"y\"\nconst b = 2\nimport z from 'w'", false, []string{"y", "w"}},
		{"shebang and bom", "\xEF\xBB\xBF#!/usr/bin/env node\nimport x from \"y\"", false, []string{"y"}},
		{"type assertion in ts", `const w = <any>window; import x from "y"`, false, []string{"y"}},
		{"generics in ts", `const m = new Map<string, Array<number>>(); import x from "y"`, false, []string{"y"}},
		{"jsx text with apostrophe", "const e = <div>Don't do this</div>;\nimport x from \"y\"", true, []string{"y"}},
		{"jsx attributes", `const e = <A b="x'" c={"}"} d={<B/>} {...p} />; import x from "y"`, true, []string{"y"}},
		{"jsx fragment nested", "const e = (<><b>it's</b><i>{`${a}`}</i></>);\nimport x from \"y\"", true, []string{"y"}},
		{"jsx generic arrow", `const f = <T,>(a: T) => a; const g = <T extends U>(a: T) => a; import x from "y"`, true, []string{"y"}},
		{"tsx generics", `const s = useState<string>(''); const m: Map<string, number> = new Map(); import x from "y"`, true, []string{"y"}},
		{"jsx in template expr", "const s = `${<div>a'b</div>}`; import x from \"y\"", true, []string{"y"}},
		{"export default jsx", "export default <h></h>\nimport x from \"y\"", true, []string{"y"}},
		{"jsx text with parens", `const e = <b>(optional)</b>; import x from "y"`, true, []string{"y"}},
		{"generic function type in tsx", "type F = <T>(a: T) => T;\ninterface I {\n  <P>(c: P): P;\n}\nexport function f(): <R>(a: R) => R;\nimport x from \"y\"", true, []string{"y"}},
		{"export import", `export import a = N.b; export import r = require("r"); import x from "y"`, false, []string{"r", "y"}},
		// Cases from the codex review of the first version.
		{"postfix increment then division", "const ratio = x++ / y;\nimport value from \"pkg\"", false, []string{"pkg"}},
		{"postfix decrement then division", "const ratio = x-- / y;\nimport value from \"pkg\"", false, []string{"pkg"}},
		{"postfix division in jsx attribute", "const node = <A value={x++ / y} />;\nimport value from \"pkg\"", true, []string{"pkg"}},
		{"regexp after if header", "if (enabled) /import fake from \"evil\"/.test(text);\nimport real from \"real\"", false, []string{"real"}},
		{"regexp after while header", "while (x) /'/.test(text);\nimport real from \"real\"", false, []string{"real"}},
		{"division after call", "const a = f(x) / 2; const s = 'q'; import x from \"y\"", false, []string{"y"}},
		{"generic signature with paren in string", "type F = <T>(value: \"(\") => T;\nimport value from \"pkg\"", true, []string{"pkg"}},
		{"generic signature with paren in comment", "type F = <T>(value: T /* ( */) => T;\nimport value from \"pkg\"", true, []string{"pkg"}},
		{"generic signature with template", "type F = <T>(value: `(${x})`) => T;\nimport value from \"pkg\"", true, []string{"pkg"}},
		{"unicode line separator", "const x = 1;\u2028import value from \"pkg\";", false, []string{"pkg"}},
		{"unicode paragraph separator ends comment", "// c\u2029import value from \"pkg\";", false, []string{"pkg"}},
		{"nbsp before import", "const x = 1;\u00a0import value from \"pkg\";", false, []string{"pkg"}},
		{"unicode identifiers", "const caf\u00e9 = 1; const \u0394 = 2; import x from \"y\"", false, []string{"y"}},
		{"jsx mismatched closing tag not jsx", "const a = 1 < b; const c = d > e; import x from \"y\"", true, []string{"y"}},
		{"jsx eof in opening tag", "const e = <div a=\"x\"", true, nil},
		{"spread in import attributes", `import data from "./d.json" with { type: "json" }; const o = {...p}; import x from "y"`, false, []string{"./d.json", "y"}},
		// Cases from the second codex review.
		{"lookahead pops pre-existing paren (no panic)", "const z = (<T>(x: T)) => T;\nimport value from \"pkg\"", true, []string{"pkg"}},
		{"unterminated regexp ends at U+2028", "const r = /unterminated\u2028import value from \"pkg\";", false, []string{"pkg"}},
		{"regexp after for await header", "for await (const x of xs) /import fake from \"evil\"/.test(x);\nimport real from \"real\"", false, []string{"real"}},
		{"regexp after for header", "for (const x of xs) /'/.test(x);\nimport real from \"real\"", false, []string{"real"}},
		{"unterminated string", "const s = 'oops\nimport x from \"y\"", false, []string{"y"}},
		{"empty", "", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, Imports([]byte(tc.src), Options{JSX: tc.jsx}))
		})
	}
}

func FuzzImports(f *testing.F) {
	for _, seed := range []string{
		`import a from "a"; const s = "x"; import b from './b'`,
		"const r = /'/; import x from \"y\"",
		"const e = <div a={x++ / 2}>it's</div>; import x from \"y\"",
		"type F = <T>(a: T) => T; import x from \"y\"",
		"`${`${1}`}` import x from \"y\"",
	} {
		f.Add(seed, true)
		f.Add(seed, false)
	}
	f.Fuzz(func(t *testing.T, src string, jsx bool) {
		// Must terminate without panicking on arbitrary input, and appending
		// a fresh statement must never make previously found imports vanish.
		got := Imports([]byte(src), Options{JSX: jsx})
		again := Imports([]byte(src+"\n;\n"), Options{JSX: jsx})
		if len(again) < len(got) {
			t.Fatalf("appending a statement lost imports: %q -> %q", got, again)
		}
	})
}
