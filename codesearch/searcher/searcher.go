package searcher

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/result"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/xiam/sexpr/ast"
	"github.com/xiam/sexpr/parser"
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

func (r *SexpSearcher) Search(rawSexpression string) ([]result.Result, error) {
	root, err := parser.Parse([]byte(rawSexpression))
	if err != nil {
		return nil, err
	}
	printTree(root)

	docIDs, err := r.indexReader.PostingQuerySX([]byte(rawSexpression))
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
