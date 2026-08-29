// Package tsimports extracts the module specifiers of static import
// declarations from TypeScript and TSX source files without a full parser.
//
// It replaces the tree-sitter (cgo) dependency that was previously used by the
// gazelle TypeScript extension. The extension only ever needed one fact per
// file: which modules the file imports with a top-level
// `import ... from "x"` declaration. Recovering that from source does not
// require a syntax tree; it requires a lexer that correctly skips everything
// in which the word `import` can appear without being an import declaration:
// comments, string literals, template literals (including nested `${}`
// expressions), regular expression literals and, for TSX, JSX text.
//
// The scanner is deliberately conservative: it tracks nesting depth and only
// recognizes `import` at the top level, so it never returns a specifier for a
// declaration inside `declare module "x" { ... }` blocks, and it ignores
// `import.meta` and dynamic `import(...)` calls.
//
// Unlike the previous tree-sitter extractor, side-effect imports
// (`import "x"`), re-exports (`export ... from "x"`) and
// `import x = require("x")` are returned too: each makes the file depend on
// the named module just as much as `import y from "x"` does.
package tsimports

import (
	"bytes"
	"unicode"
	"unicode/utf8"
)

// Options controls scanning.
type Options struct {
	// JSX enables JSX element scanning. Use it for .tsx files. In .ts files
	// `<T>expr` is a type assertion and must not be treated as JSX.
	JSX bool
}

// Imports returns the module specifiers of the static import declarations
// found at the top level of src, in source order. Duplicates are preserved.
func Imports(src []byte, opts Options) []string {
	s := &scanner{src: src, jsx: opts.JSX}
	var out []string
	var last token
	for {
		t := s.next()
		if t.kind == tokEOF {
			break
		}
		// `x.import` / `x.export` are property accesses, not declarations.
		isDecl := t.kind == tokWord && (t.text == "import" || t.text == "export") && s.depth == 0 &&
			!(last.kind == tokPunct && last.text == ".")
		last = t
		if !isDecl {
			continue
		}
		var spec string
		var ok bool
		if t.text == "import" {
			spec, ok = s.importSpecifier()
		} else {
			spec, ok = s.exportSpecifier()
		}
		if ok {
			out = append(out, spec)
		}
		last = s.prev
	}
	return out
}

// importSpecifier is called right after an `import` keyword at top level and
// consumes the rest of the declaration, returning its module specifier.
func (s *scanner) importSpecifier() (string, bool) {
	t := s.next()
	switch t.kind {
	case tokString:
		// import "side-effect";
		return t.text, true
	case tokPunct:
		if t.text == "(" || t.text == "." {
			// Dynamic import or import.meta: not a declaration.
			return "", false
		}
	case tokEOF:
		return "", false
	default:
	}
	// import [type] [default][, ] [* as ns | { ... }] from "spec" [with {...}]
	// import [type] x = require("spec")
	importClauseBraceDepth := 0
	for {
		switch t.kind {
		case tokEOF:
			return "", false
		case tokPunct:
			switch t.text {
			case "{":
				importClauseBraceDepth++
			case "}":
				importClauseBraceDepth--
			case ";":
				if importClauseBraceDepth == 0 {
					return "", false // malformed declaration
				}
			case "=":
				if importClauseBraceDepth == 0 {
					return s.requireSpecifier()
				}
			}
		case tokWord:
			if importClauseBraceDepth == 0 && t.text == "from" {
				return s.stringNext()
			}
			if importClauseBraceDepth == 0 && (t.text == "import" || t.text == "export") {
				// Malformed declaration ran into the next one; re-scan it.
				s.pushBack(t)
				return "", false
			}
		default:
		}
		t = s.next()
	}
}

// exportSpecifier is called right after an `export` keyword at top level. It
// returns the module specifier of a re-export (`export * from "x"`,
// `export * as ns from "x"`, `export { a, b as c } from "x"`,
// `export type { T } from "x"`) and consumes only as much as needed to tell
// that the statement is not one; `export import x = require("y")` is
// handled by returning to the main loop, which sees the `import`.
func (s *scanner) exportSpecifier() (string, bool) {
	t := s.next()
	if t.kind == tokWord && t.text == "type" {
		t = s.next()
	}
	switch {
	case t.kind == tokPunct && t.text == "*":
		for {
			t = s.next()
			switch {
			case t.kind == tokEOF:
				return "", false
			case t.kind == tokWord && t.text == "from":
				return s.stringNext()
			case t.kind == tokPunct && t.text == ";":
				return "", false
			}
		}
	case t.kind == tokPunct && t.text == "{":
		braceDepth := 1
		for braceDepth > 0 {
			t = s.next()
			switch {
			case t.kind == tokEOF:
				return "", false
			case t.kind == tokPunct && t.text == "{":
				braceDepth++
			case t.kind == tokPunct && t.text == "}":
				braceDepth--
			}
		}
		t = s.next()
		if t.kind == tokWord && t.text == "from" {
			return s.stringNext()
		}
		s.pushBack(t)
		return "", false
	default:
		// export const/function/class/default/import ...: rescan the token.
		s.pushBack(t)
		return "", false
	}
}

// requireSpecifier handles the remainder of `import x = require("spec")`.
// `import x = N.y` (a namespace alias) has no module specifier.
func (s *scanner) requireSpecifier() (string, bool) {
	t := s.next()
	if t.kind != tokWord || t.text != "require" {
		s.pushBack(t)
		return "", false
	}
	if t = s.next(); t.kind != tokPunct || t.text != "(" {
		s.pushBack(t)
		return "", false
	}
	return s.stringNext()
}

// stringNext returns the next token's text if it is a string literal.
func (s *scanner) stringNext() (string, bool) {
	t := s.next()
	if t.kind == tokString {
		return t.text, true
	}
	s.pushBack(t)
	return "", false
}

type tokKind int

const (
	tokEOF tokKind = iota
	tokWord
	tokPunct
	tokString   // text is the raw contents between the quotes
	tokTemplate // template literal (contents already skipped)
	tokRegexp
	tokNumber
	tokJSX // a whole JSX element (contents already skipped)
)

type token struct {
	kind tokKind
	text string
	// ctrlClose marks a `)` that closes an `if (...)`, `while (...)`,
	// `for (...)` or `with (...)` header: a `/` after it starts a regexp
	// (`if (x) /re/.test(y)`), whereas after any other `)` it is division.
	ctrlClose bool
}

type scanner struct {
	src    []byte
	pos    int
	prev   token // previous significant token (for regexp/JSX disambiguation)
	depth  int   // (), [], {} nesting depth at top level
	jsx    bool
	pushed *token
	// parens records, for each currently open `(`, whether it started a
	// control-flow header.
	parens []bool
}

type scannerState struct {
	pos    int
	prev   token
	depth  int
	pushed *token
	parens int
}

func (s *scanner) save() scannerState {
	return scannerState{s.pos, s.prev, s.depth, s.pushed, len(s.parens)}
}

func (s *scanner) restore(st scannerState) {
	s.pos, s.prev, s.depth, s.pushed = st.pos, st.prev, st.depth, st.pushed
	s.parens = s.parens[:st.parens]
}

func (s *scanner) pushBack(t token) { s.pushed = &t }

// next returns the next significant token, skipping whitespace and comments.
func (s *scanner) next() token {
	if s.pushed != nil {
		t := *s.pushed
		s.pushed = nil
		return t
	}
	return s.scanAndTrack()
}

func (s *scanner) scanAndTrack() token {
	t := s.scan()
	s.prev = t
	return t
}

// Multi-character punctuators, longest first. The ones that matter for
// disambiguation are `++`/`--` (a `/` after them is division), `=>` (arrow
// functions and generic signatures) and `...`; the rest just keep tokens
// honest.
var punctuators = []string{
	">>>=", "...", "===", "!==", "**=", "<<=", ">>=", ">>>", "&&=", "||=", "??=",
	"=>", "++", "--", "**", "&&", "||", "??", "?.", "==", "!=", "<=", ">=",
	"<<", ">>", "+=", "-=", "*=", "/=", "%=", "&=", "|=", "^=",
}

func (s *scanner) scan() token {
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			s.pos++
		case c >= 0x80:
			r, size := utf8.DecodeRune(s.src[s.pos:])
			if isUnicodeSpace(r) {
				s.pos += size
				continue
			}
			return s.scanWord()
		case c == '#' && s.pos == 0 && s.hasPrefix("#!"): // shebang
			s.skipLine()
		case c == '/' && s.hasPrefix("//"):
			s.skipLine()
		case c == '/' && s.hasPrefix("/*"):
			s.skipBlockComment()
		case c == '"' || c == '\'':
			return token{kind: tokString, text: s.scanString(c)}
		case c == '`':
			s.scanTemplate()
			return token{kind: tokTemplate}
		case c == '/':
			if s.regexpAllowed() {
				s.scanRegexp()
				return token{kind: tokRegexp}
			}
			return s.scanPunct()
		case c == '<' && s.jsx && s.jsxAllowed():
			if s.scanJSXElement() {
				return token{kind: tokJSX}
			}
			// Not JSX (e.g. `<T,>(x: T) => x`): treat `<` as a punctuator.
			return s.scanPunct()
		case isIdentStart(c):
			return s.scanWord()
		case c >= '0' && c <= '9':
			start := s.pos
			for s.pos < len(s.src) && (isIdentPart(s.src[s.pos]) || s.src[s.pos] == '.') {
				s.pos++
			}
			return token{kind: tokNumber, text: string(s.src[start:s.pos])}
		default:
			return s.scanPunct()
		}
	}
	return token{kind: tokEOF}
}

func (s *scanner) scanWord() token {
	start := s.pos
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		if c < 0x80 {
			if !isIdentPart(c) {
				break
			}
			s.pos++
			continue
		}
		r, size := utf8.DecodeRune(s.src[s.pos:])
		if isUnicodeSpace(r) {
			break
		}
		s.pos += size
	}
	return token{kind: tokWord, text: string(s.src[start:s.pos])}
}

func (s *scanner) scanPunct() token {
	for _, p := range punctuators {
		if s.hasPrefix(p) {
			s.pos += len(p)
			return token{kind: tokPunct, text: p}
		}
	}
	c := s.src[s.pos]
	s.pos++
	t := token{kind: tokPunct, text: string(c)}
	switch c {
	case '(':
		s.depth++
		s.parens = append(s.parens, s.prev.kind == tokWord && isControlKeyword(s.prev.text))
	case ')':
		if s.depth > 0 {
			s.depth--
		}
		if n := len(s.parens); n > 0 {
			t.ctrlClose = s.parens[n-1]
			s.parens = s.parens[:n-1]
		}
	case '[', '{':
		s.depth++
	case ']', '}':
		if s.depth > 0 {
			s.depth--
		}
	}
	return t
}

func isControlKeyword(w string) bool {
	switch w {
	case "if", "while", "for", "with":
		return true
	}
	return false
}

func (s *scanner) hasPrefix(p string) bool {
	return bytes.HasPrefix(s.src[s.pos:], []byte(p))
}

// atLineTerminator reports whether a line terminator (LF, CR, U+2028 or
// U+2029) starts at s.pos.
func (s *scanner) atLineTerminator() bool {
	c := s.src[s.pos]
	if c == '\n' || c == '\r' {
		return true
	}
	return c == 0xE2 && s.pos+2 < len(s.src) && s.src[s.pos+1] == 0x80 && (s.src[s.pos+2] == 0xA8 || s.src[s.pos+2] == 0xA9)
}

func (s *scanner) skipLine() {
	for s.pos < len(s.src) && !s.atLineTerminator() {
		s.pos++
	}
}

func (s *scanner) skipBlockComment() {
	s.pos += 2
	for s.pos < len(s.src) {
		if s.src[s.pos] == '*' && s.pos+1 < len(s.src) && s.src[s.pos+1] == '/' {
			s.pos += 2
			return
		}
		s.pos++
	}
}

// scanString consumes a quoted string starting at s.pos and returns its raw
// contents. Unterminated strings end at the line terminator (like the
// TypeScript scanner, which reports an error but recovers there).
func (s *scanner) scanString(quote byte) string {
	s.pos++
	start := s.pos
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		if c == '\\' {
			s.pos += 2
			continue
		}
		if c == quote {
			text := string(s.src[start:s.pos])
			s.pos++
			return text
		}
		if s.atLineTerminator() {
			return string(s.src[start:s.pos])
		}
		s.pos++
	}
	return string(s.src[start:min(s.pos, len(s.src))])
}

// scanTemplate consumes a template literal, including nested `${...}`
// expressions (which may themselves contain templates, strings, regexps...).
func (s *scanner) scanTemplate() {
	s.pos++ // opening backtick
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		switch {
		case c == '\\':
			s.pos += 2
		case c == '`':
			s.pos++
			return
		case c == '$' && s.pos+1 < len(s.src) && s.src[s.pos+1] == '{':
			s.pos += 2
			s.skipBalanced()
		default:
			s.pos++
		}
	}
}

// skipBalanced scans tokens until the `}` that closes an already-consumed `{`.
// It is used for `${...}` in templates and `{...}` in JSX.
func (s *scanner) skipBalanced() {
	saveDepth := s.depth
	s.depth = 1
	savePrev := s.prev
	s.prev = token{kind: tokPunct, text: "{"}
	for s.depth > 0 {
		if s.scanAndTrack().kind == tokEOF {
			break
		}
	}
	s.depth = saveDepth
	s.prev = savePrev
}

// regexpAllowed reports whether a `/` at the current position starts a
// regular expression literal rather than a division operator, based on the
// previous significant token.
func (s *scanner) regexpAllowed() bool {
	switch s.prev.kind {
	case tokEOF:
		return true
	case tokNumber, tokString, tokTemplate, tokRegexp, tokJSX:
		return false
	case tokWord:
		switch s.prev.text {
		case "return", "typeof", "instanceof", "in", "of", "new", "delete",
			"void", "throw", "case", "do", "else", "yield", "await", "extends":
			return true
		}
		return false
	case tokPunct:
		switch s.prev.text {
		case ")":
			return s.prev.ctrlClose
		case "]", "++", "--":
			return false
		}
		// `}` is ambiguous (end of block vs. end of object literal). A regexp
		// after a block is far more common than dividing an object literal.
		return true
	default:
		return true
	}
}

func (s *scanner) scanRegexp() {
	s.pos++ // opening slash
	inClass := false
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		switch {
		case c == '\\':
			s.pos += 2
			continue
		case c == '\n' || c == '\r':
			return // unterminated
		case c == '[':
			inClass = true
		case c == ']':
			inClass = false
		case c == '/' && !inClass:
			s.pos++
			for s.pos < len(s.src) && isIdentPart(s.src[s.pos]) { // flags
				s.pos++
			}
			return
		}
		s.pos++
	}
}

// jsxAllowed reports whether a `<` at the current position can start a JSX
// element: only in expression-start position, where a comparison or a generic
// type argument list cannot appear.
func (s *scanner) jsxAllowed() bool {
	switch s.prev.kind {
	case tokEOF:
		return true
	case tokWord:
		switch s.prev.text {
		case "return", "yield", "await", "case", "do", "else", "throw", "typeof", "void", "in", "of", "new", "delete", "extends", "default":
			return true
		}
		return false
	case tokPunct:
		switch s.prev.text {
		case ")", "]", "}", "++", "--":
			// `}` closes a block or object literal; JSX is only valid after
			// `}` inside a JSX expression container, which skipBalanced
			// handles by starting with prev="{".
			return false
		}
		return true
	default:
		return false
	}
}

// scanJSXElement consumes a complete JSX element or fragment starting at `<`.
// It returns false (without consuming anything) if the text does not look
// like well-formed JSX, e.g. an arrow-function generic `<T,>`, a generic
// function type `<T>(x: T) => T`, or an element whose tags do not match.
func (s *scanner) scanJSXElement() bool {
	st := s.save()
	if !s.scanJSXTag(true) {
		s.restore(st)
		return false
	}
	return true
}

func isJSXNameByte(c byte) bool {
	return isIdentPart(c) || c == '.' || c == ':' || c == '-'
}

// scanJSXTag consumes an opening tag (self-closing or not) and, if not
// self-closing, its children and matching closing tag. Returns false if the
// text is not well-formed JSX.
func (s *scanner) scanJSXTag(first bool) bool {
	s.pos++ // '<'
	s.skipSpace()
	if first && s.pos < len(s.src) && s.src[s.pos] == '/' {
		// A stray closing tag (`</h>` after a `<h>` that was not treated as
		// JSX) is never the start of an element.
		return false
	}
	// Tag name: identifiers with '.', ':', '-'; empty for fragments.
	nameStart := s.pos
	for s.pos < len(s.src) && isJSXNameByte(s.src[s.pos]) {
		s.pos++
	}
	name := s.src[nameStart:s.pos]
	s.skipSpace()
	if first && len(name) > 0 && s.pos < len(s.src) {
		// Distinguish `<T,>(...)` / `<T extends U>(...)` / `<T = U>` generics.
		if s.src[s.pos] == ',' || s.src[s.pos] == '=' && !(s.pos+1 < len(s.src) && s.src[s.pos+1] == '>') || s.hasPrefix("extends ") {
			return false
		}
	}
	// Attributes.
	selfClosing := false
attrs:
	for {
		s.skipSpace()
		if s.pos >= len(s.src) {
			return false // EOF inside an opening tag: not JSX
		}
		c := s.src[s.pos]
		switch {
		case c == '>':
			s.pos++
			if first && s.isGenericCallSignature() {
				// `<T>(x: T) => T` / `<T>(x: T): U` is a generic function
				// type (valid in type positions even in .tsx), not an element.
				return false
			}
			break attrs
		case c == '/' && s.pos+1 < len(s.src) && s.src[s.pos+1] == '>':
			s.pos += 2
			selfClosing = true
			break attrs
		case c == '{':
			s.pos++
			s.skipBalanced()
		case c == '"' || c == '\'':
			s.scanString(c)
		case c == '=':
			s.pos++
		case c == '<':
			// e.g. generic element type args `<Foo<T>>`: skip until '>'.
			for s.pos < len(s.src) && s.src[s.pos] != '>' {
				s.pos++
			}
			s.pos++
		case c == '/' && s.hasPrefix("//"):
			s.skipLine()
		case c == '/' && s.hasPrefix("/*"):
			s.skipBlockComment()
		default:
			s.pos++
		}
	}
	if selfClosing {
		return true
	}
	// Children, until the matching closing tag.
	for s.pos < len(s.src) {
		switch s.src[s.pos] {
		case '{':
			s.pos++
			s.skipBalanced()
		case '<':
			if s.pos+1 < len(s.src) && s.src[s.pos+1] == '/' {
				s.pos += 2
				s.skipSpace()
				closeStart := s.pos
				for s.pos < len(s.src) && isJSXNameByte(s.src[s.pos]) {
					s.pos++
				}
				if !bytes.Equal(s.src[closeStart:s.pos], name) {
					return false // mismatched closing tag: not JSX
				}
				s.skipSpace()
				if s.pos >= len(s.src) || s.src[s.pos] != '>' {
					return false
				}
				s.pos++
				return true
			}
			if !s.scanJSXTag(false) {
				return false
			}
		default:
			s.pos++ // JSX text
		}
	}
	return false // EOF before the closing tag
}

// isGenericCallSignature reports whether the text after a just-consumed
// `<Name>` is `(...) =>` or `(...):`, i.e. a generic function type or call
// signature rather than JSX children. It uses the real lexer for the
// lookahead (so strings, comments, templates and nested parens inside the
// parameter list are handled) and consumes nothing.
func (s *scanner) isGenericCallSignature() bool {
	st := s.save()
	defer s.restore(st)
	s.prev = token{kind: tokPunct, text: ">"}
	if t := s.scanAndTrack(); t.kind != tokPunct || t.text != "(" {
		return false
	}
	depth := 1
	for depth > 0 {
		t := s.scanAndTrack()
		switch {
		case t.kind == tokEOF, t.kind == tokJSX:
			return false
		case t.kind == tokPunct && t.text == "(":
			depth++
		case t.kind == tokPunct && t.text == ")":
			depth--
		}
	}
	t := s.scanAndTrack()
	return t.kind == tokPunct && (t.text == ":" || t.text == "=>")
}

func (s *scanner) skipSpace() {
	for s.pos < len(s.src) {
		switch s.src[s.pos] {
		case ' ', '\t', '\n', '\r':
			s.pos++
		default:
			return
		}
	}
}

func isIdentStart(c byte) bool {
	return c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || c >= '0' && c <= '9'
}

// isUnicodeSpace reports whether a non-ASCII rune is ECMAScript WhiteSpace
// or a LineTerminator (U+FEFF, the Zs category, U+2028, U+2029).
func isUnicodeSpace(r rune) bool {
	return r == 0xFEFF || r == 0x2028 || r == 0x2029 || unicode.Is(unicode.Zs, r)
}
