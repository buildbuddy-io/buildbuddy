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
// `import.meta`, dynamic `import(...)` calls, `export ... from` re-exports and
// `import x = require("y")` (matching the previous tree-sitter behaviour).
package tsimports

import (
	"strings"
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
		// `x.import` is a property access and `export import x = ...` is an
		// alias export (an export_statement to tree-sitter), not a declaration.
		isImport := t.kind == tokWord && t.text == "import" && s.depth == 0 &&
			!(last.kind == tokPunct && last.text == ".") &&
			!(last.kind == tokWord && last.text == "export")
		last = t
		if !isImport {
			continue
		}
		if spec, ok := s.importSpecifier(); ok {
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
		// import "side-effect"; tree-sitter's import_statement has a single
		// named child here, which the previous extractor skipped. Keep parity.
		return "", false
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
	groupDepth := 0
	for {
		switch t.kind {
		case tokEOF:
			return "", false
		case tokPunct:
			switch t.text {
			case "{":
				groupDepth++
			case "}":
				groupDepth--
			case ";", "=":
				if groupDepth == 0 {
					// `;` terminates a malformed declaration; `=` means
					// `import x = require("y")` / `import x = N.y`, which the
					// previous extractor ignored.
					return "", false
				}
			}
		case tokWord:
			if groupDepth == 0 && t.text == "from" {
				t = s.next()
				if t.kind == tokString {
					return t.text, true
				}
				return "", false
			}
			if groupDepth == 0 && (t.text == "import" || t.text == "export") {
				// Malformed declaration ran into the next one; re-scan it.
				s.pushBack(t)
				return "", false
			}
		default:
		}
		t = s.next()
	}
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
}

type scanner struct {
	src    []byte
	pos    int
	prev   token // previous significant token (for regexp/JSX disambiguation)
	depth  int   // (), [], {} nesting depth at top level
	jsx    bool
	pushed *token
}

func (s *scanner) pushBack(t token) { s.pushed = &t }

// next returns the next significant token, skipping whitespace and comments.
func (s *scanner) next() token {
	if s.pushed != nil {
		t := *s.pushed
		s.pushed = nil
		return t
	}
	t := s.scan()
	s.prev = t
	return t
}

func (s *scanner) scan() token {
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			s.pos++
		case c == 0xEF && s.hasPrefix("\xEF\xBB\xBF"): // BOM
			s.pos += 3
		case c == '#' && s.pos == 0 && s.hasPrefix("#!"): // shebang
			s.skipLine()
		case c == '/' && s.hasPrefix("//"):
			s.skipLine()
		case c == '/' && s.hasPrefix("/*"):
			s.skipBlockComment()
		case c == '"' || c == '\'':
			return token{tokString, s.scanString(c)}
		case c == '`':
			s.scanTemplate()
			return token{tokTemplate, ""}
		case c == '/':
			if s.regexpAllowed() {
				s.scanRegexp()
				return token{tokRegexp, ""}
			}
			s.pos++
			if s.pos < len(s.src) && s.src[s.pos] == '=' {
				s.pos++
				return token{tokPunct, "/="}
			}
			return token{tokPunct, "/"}
		case c == '<' && s.jsx && s.jsxAllowed():
			if s.scanJSXElement() {
				return token{tokJSX, ""}
			}
			// Not JSX (e.g. `<T,>(x: T) => x`): treat `<` as a punctuator.
			s.pos++
			return token{tokPunct, "<"}
		case isIdentStart(c):
			start := s.pos
			for s.pos < len(s.src) && isIdentPart(s.src[s.pos]) {
				s.pos++
			}
			return token{tokWord, string(s.src[start:s.pos])}
		case c >= '0' && c <= '9':
			start := s.pos
			for s.pos < len(s.src) && (isIdentPart(s.src[s.pos]) || s.src[s.pos] == '.') {
				s.pos++
			}
			return token{tokNumber, string(s.src[start:s.pos])}
		default:
			s.pos++
			switch c {
			case '(', '[', '{':
				s.depth++
			case ')', ']', '}':
				if s.depth > 0 {
					s.depth--
				}
			}
			return token{tokPunct, string(c)}
		}
	}
	return token{tokEOF, ""}
}

func (s *scanner) hasPrefix(p string) bool {
	return strings.HasPrefix(string(s.src[s.pos:min(len(s.src), s.pos+len(p))]), p)
}

func (s *scanner) skipLine() {
	for s.pos < len(s.src) && s.src[s.pos] != '\n' {
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
// contents. Unterminated strings end at the newline (like the TypeScript
// scanner, which reports an error but recovers there).
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
		if c == '\n' {
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
	s.prev = token{tokPunct, "{"}
	for s.depth > 0 {
		if s.scanAndTrack().kind == tokEOF {
			break
		}
	}
	s.depth = saveDepth
	s.prev = savePrev
}

func (s *scanner) scanAndTrack() token {
	t := s.scan()
	s.prev = t
	return t
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
		case ")", "]":
			return false
		}
		// `}` is ambiguous (end of block vs. end of object literal). A regexp
		// after a block is far more common than dividing an object literal.
		return true
	}
	return true
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
		case c == '\n':
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
		case ")", "]", "}":
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
// like JSX, e.g. an arrow-function generic `<T,>` or `<T extends U>`.
func (s *scanner) scanJSXElement() bool {
	start := s.pos
	if !s.scanJSXTag(true) {
		s.pos = start
		return false
	}
	return true
}

// scanJSXTag consumes an opening tag (self-closing or not) and, if not
// self-closing, its children and closing tag. Returns false if the text is
// not JSX.
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
	for s.pos < len(s.src) && (isIdentPart(s.src[s.pos]) || s.src[s.pos] == '.' || s.src[s.pos] == ':' || s.src[s.pos] == '-') {
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
	for {
		s.skipSpace()
		if s.pos >= len(s.src) {
			return true
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
			goto children
		case c == '/' && s.pos+1 < len(s.src) && s.src[s.pos+1] == '>':
			s.pos += 2
			selfClosing = true
			goto children
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
children:
	if selfClosing {
		return true
	}
	for s.pos < len(s.src) {
		switch s.src[s.pos] {
		case '{':
			s.pos++
			s.skipBalanced()
		case '<':
			if s.pos+1 < len(s.src) && s.src[s.pos+1] == '/' {
				// Closing tag: </name> or </>.
				for s.pos < len(s.src) && s.src[s.pos] != '>' {
					s.pos++
				}
				s.pos++
				return true
			}
			s.scanJSXTag(false)
		default:
			s.pos++ // JSX text
		}
	}
	return true
}

// isGenericCallSignature reports whether the text after a just-consumed
// `<Name>` looks like `(...) =>` or `(...):`, i.e. a generic function type or
// call signature rather than JSX children. It does not consume anything.
func (s *scanner) isGenericCallSignature() bool {
	i := s.pos
	for i < len(s.src) && (s.src[i] == ' ' || s.src[i] == '\t' || s.src[i] == '\n' || s.src[i] == '\r') {
		i++
	}
	if i >= len(s.src) || s.src[i] != '(' {
		return false
	}
	depth := 0
	for ; i < len(s.src); i++ {
		switch s.src[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				i++
				for i < len(s.src) && (s.src[i] == ' ' || s.src[i] == '\t' || s.src[i] == '\n' || s.src[i] == '\r') {
					i++
				}
				return i < len(s.src) && (s.src[i] == ':' || s.src[i] == '=' && i+1 < len(s.src) && s.src[i+1] == '>')
			}
		case '<':
			// Children text of a real element would end at `</`; a call
			// signature never contains one.
			if i+1 < len(s.src) && s.src[i+1] == '/' {
				return false
			}
		}
	}
	return false
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
	return c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || c >= '0' && c <= '9'
}
