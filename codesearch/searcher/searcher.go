package searcher

import (
	"fmt"
	"strings"
	
	"github.com/buildbuddy-io/buildbuddy/codesearch/result"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/xiam/sexpr/ast"
	"github.com/xiam/sexpr/parser"
	"golang.org/x/exp/slices"
)

type Searcher interface {
	Search(rawQuery string) ([]result.Result, error)
}

type SexpSearcher struct {
	indexReader types.IndexReader
}
func NewSexp(ir types.IndexReader) *SexpSearcher {
	return &SexpSearcher{indexReader: ir}
}

func printTree(node *ast.Node) {
  printIndentedTree(node, 0)
}

func printIndentedTree(node *ast.Node, indentationLevel int) {
  indent := strings.Repeat("  ", indentationLevel)
  if node.IsVector() {
    fmt.Printf("%s<%s>\n", indent, node.Type())
    children := node.List()
    for i := range children {
      printIndentedTree(children[i], indentationLevel+1)
    }
    fmt.Printf("%s</%s>\n", indent, node.Type())
    return
  }
  fmt.Printf("%s<%s>%v</%s>\n", indent, node.Type(), node.Value(), node.Type())
}

const (
	QAnd = ":and"
	QOr  = ":or"
	QEq  = ":eq"
)

var allowedAtoms = []string{QEq, QAnd, QOr}

func (r *SexpSearcher) validate(node *ast.Node) error {
	if node.IsVector() {
		for _, child := range node.List() {
			if err := r.validate(child); err != nil {
				return err
			}
		}
	}
	if node.Type() == ast.NodeTypeAtom {
		atomString, ok := node.Value().(string)
		if !ok {
			return status.InvalidArgumentErrorf("Query atom: %q not string", node.Value())
		}
		if !slices.Contains(allowedAtoms, atomString) {
			return status.InvalidArgumentErrorf("Unknown query atom: %q", node.Value())
		}
	}
	return nil
}

func isNestedExpression(node *ast.Node) bool {
	if node.Type() != ast.NodeTypeExpression {
		return false
	}
	for _, child := range node.List() {
		if child.Type() == ast.NodeTypeExpression {
			return true
		}
	}
	return false
}

func (r *SexpSearcher) qeq(children []*ast.Node) ([]uint64, error) {
	if len(children) != 3 {
		return nil, status.InvalidArgumentErrorf("%s expression should have 3 elements: %q (has %d)", QEq, children, len(children))
	}
	field, ok := children[1].Value().(string)
	if !ok {
		return nil, status.InvalidArgumentErrorf("field name %q must be a string", children[1])
	}
	ngram, ok := children[2].Value().(string)
	if !ok {
		return nil, status.InvalidArgumentErrorf("ngram %q must be a string/bytes", children[2])
	}
	return r.indexReader.PostingListF([]byte(ngram), field)
}

func (r *SexpSearcher) evalSingleExpression(node *ast.Node) ([]uint64, error) {
	children := node.List()
	if len(children) == 0 {
		return nil, status.FailedPreconditionErrorf("empty expression: %q", node)
	}
	atom := children[0]
	if atom.Type() != ast.NodeTypeAtom {
		return nil, status.FailedPreconditionErrorf("first element of expression %q was not query atom", node)
	}
	switch atom.Value().(string) {
	case QEq:
		return r.qeq(children)
	default:
		return nil, status.InternalErrorf("Unsupported query atom %q", atom)
	}
}

func (r *SexpSearcher) Search(rawSexpression string) ([]result.Result, error) {
	root, err := parser.Parse([]byte(rawSexpression))
	if err != nil {
		return nil, err
	}

	// unwrap the tree of expressions which is wrapped in a list.
	if root.Type() == ast.NodeTypeList && len(root.List()) == 1 {
		root = root.List()[0]
	}
	
	if err := r.validate(root); err != nil {
		return nil, err
	}
	printTree(root)

	docIDs, err := r.evalSingleExpression(root)
	if err != nil {
		return nil, err
	}

	for _, docID := range docIDs {
		doc, err := r.indexReader.GetStoredDocument(docID)
		if err != nil {
			return nil, err
		}
		log.Printf("docID: %d %s", docID, doc.Field("filename").Contents())
	}
	return nil, nil
}

func (r *SexpSearcher) retrieve(docids []uint64, fields []string) ([]types.Document, error) {
	return nil, nil
}
