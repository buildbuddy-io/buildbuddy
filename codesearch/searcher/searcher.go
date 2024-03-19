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

func printAST(node *ast.Node, indentationLevel int) {
	indent := strings.Repeat("  ", indentationLevel)
	if node.IsVector() {
		fmt.Printf("%s<%s>\n", indent, node.Type())
		children := node.List()
		for i := range children {
			printAST(children[i], indentationLevel+1)
		}
		fmt.Printf("%s</%s>\n", indent, node.Type())
		return
	}
	fmt.Printf("%s<%s>%v</%s>\n", indent, node.Type(), node.Value(), node.Type())
}

func (r *SexpSearcher) Search(squery string) ([]result.Result, error) {
	root, err := parser.Parse([]byte(squery))
	if err != nil {
		return nil, err
	}
	printAST(root, 0)

	docIDs, err := r.indexReader.RawQuery([]byte(squery))
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
