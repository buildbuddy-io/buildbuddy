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
		{"side effect import ignored (parity)", `import "./polyfill"; import x from "y"`, false, []string{"y"}},
		{"require ignored (parity)", `import fs = require("fs"); import x from "y"`, false, []string{"y"}},
		{"namespace alias ignored", `import A = N.B; import x from "y"`, false, []string{"y"}},
		{"dynamic import ignored", `const m = import("dyn"); import x from "y"`, false, []string{"y"}},
		{"import.meta", `const u = import.meta.url; import x from "y"`, false, []string{"y"}},
		{"export from ignored (parity)", `export * from "a"; export { b } from "b"; import x from "y"`, false, []string{"y"}},
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
		{"export import ignored (parity)", `export import a = N.b; import x from "y"`, false, []string{"y"}},
		{"unterminated string", "const s = 'oops\nimport x from \"y\"", false, []string{"y"}},
		{"empty", "", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, Imports([]byte(tc.src), Options{JSX: tc.jsx}))
		})
	}
}
