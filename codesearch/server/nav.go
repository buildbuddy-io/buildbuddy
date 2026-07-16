package server

import (
	"context"
	"net/url"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/navindex"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	srpb "github.com/buildbuddy-io/buildbuddy/proto/search"
	kcpb "kythe.io/kythe/proto/common_go_proto"
	kxpb "kythe.io/kythe/proto/xref_go_proto"
)

// Serve Go click-through navigation from tree-sitter annotations instead of
// kythe. This is the kythe-free, syntactic nav tier — it needs only indexed
// file contents and the existing symbols/import_id posting lists, no kythe
// ingestion pipeline. There is no enabling flag: KytheProxy routes a
// Decorations/CrossReferences request here precisely when its ticket(s) carry
// the tree-sitter:// scheme (see decorationsUsesTreeSitter / annotations.HasScheme).
var (
	// The kythe Decorations/CrossReferences endpoints carry no namespace (a
	// kythe ticket is group-scoped). The index is namespace-scoped, so the
	// prototype reads nav data from a single configured namespace within the
	// authenticated group. A proper version would carry repo/namespace on the
	// request, as the prior design notes flag.
	treeSitterNavNamespace = flag.String("codesearch.experimental_treesitter_nav_namespace", "",
		"Namespace (within the authenticated group) to serve experimental tree-sitter nav from.")
)

// decorationsUsesTreeSitter reports whether a Decorations request should be
// served from tree-sitter (vs kythe), based solely on its file ticket scheme.
func decorationsUsesTreeSitter(req *kxpb.DecorationsRequest) bool {
	return annotations.HasScheme(req.GetLocation().GetTicket())
}

// crossReferencesUsesTreeSitter reports whether a CrossReferences request
// should be served from tree-sitter, based on its target ticket scheme. The
// tickets in one request all originate from a single decoration set, so they
// share a scheme; checking any is sufficient.
func crossReferencesUsesTreeSitter(req *kxpb.CrossReferencesRequest) bool {
	return slices.ContainsFunc(req.GetTicket(), annotations.HasScheme)
}

// docsUsesTreeSitter reports whether a documentation request should be served
// from tree-sitter, based on its ticket scheme.
func docsUsesTreeSitter(req *srpb.ExtendedDocumentationRequest) bool {
	return annotations.HasScheme(req.GetTicket())
}

// extendedXrefsUsesTreeSitter reports whether a references-panel request should
// be served from tree-sitter, based on its ticket scheme.
func extendedXrefsUsesTreeSitter(req *srpb.ExtendedXrefsRequest) bool {
	return slices.ContainsFunc(req.GetTickets(), annotations.HasScheme)
}

// tsNavDecorations answers a Decorations request from tree-sitter: it loads the
// viewed file from the index and returns a clickable reference for every
// resolvable identifier occurrence. Unknown files (or non-Go files, which
// produce no refs) yield an empty reply, never an error, so the frontend
// simply shows no decorations.
func (css *codesearchServer) tsNavDecorations(ctx context.Context, req *kxpb.DecorationsRequest) (*kxpb.DecorationsReply, error) {
	reply := &kxpb.DecorationsReply{Location: req.GetLocation()}

	path := pathFromTicket(req.GetLocation().GetTicket())
	if path == "" {
		return reply, nil
	}
	r, err := css.navReader(ctx)
	if err != nil {
		return nil, err
	}
	f, ok := navindex.FindFile(ctx, r, path)
	if !ok {
		return reply, nil
	}

	refs, err := annotations.Decorate(ctx, f.Lang, f.Content, annotations.NavOptions{
		SelfImportID: f.SelfImportID,
		Path:         path,
		InRepo:       f.InRepo,
	})
	if err != nil {
		log.CtxInfof(ctx, "nav: decorate %q failed: %s", path, err)
		return reply, nil
	}
	for _, rf := range refs {
		reply.Reference = append(reply.Reference, &kxpb.DecorationsReply_Reference{
			TargetTicket: rf.TargetTicket,
			Kind:         rf.Kind,
			Span:         navSpan(rf.Start, rf.End),
		})
	}
	return reply, nil
}

// tsNavCrossReferences resolves each requested target ticket to its definition
// site(s) via the index's symbols/import_id posting lists, returning kythe
// CrossReferenceSets the frontend navigates to. Tickets the nav layer did not
// mint (e.g. real kythe tickets) resolve to nothing and are simply omitted.
func (css *codesearchServer) tsNavCrossReferences(ctx context.Context, req *kxpb.CrossReferencesRequest) (*kxpb.CrossReferencesReply, error) {
	r, err := css.navReader(ctx)
	if err != nil {
		return nil, err
	}
	lk := &navindex.DefLookup{R: r}

	reply := &kxpb.CrossReferencesReply{
		CrossReferences: make(map[string]*kxpb.CrossReferencesReply_CrossReferenceSet),
	}
	for _, ticket := range req.GetTicket() {
		locs, err := annotations.Resolve(ctx, lk, ticket)
		if err != nil {
			return nil, err
		}
		if len(locs) == 0 {
			continue
		}
		set := &kxpb.CrossReferencesReply_CrossReferenceSet{Ticket: ticket}
		for _, loc := range locs {
			set.Definition = append(set.Definition, &kxpb.CrossReferencesReply_RelatedAnchor{
				Anchor: &kxpb.Anchor{
					Parent: ticketForPath(loc.Path),
					Span:   navSpan(loc.Start, loc.End),
				},
			})
		}
		reply.CrossReferences[ticket] = set
	}
	return reply, nil
}

// tsNavDocumentation answers a hover request from tree-sitter: it resolves the
// ticket to its definition and returns the declaration's kind, signature, and
// leading doc comment. The frontend renders nothing unless both node info and
// a definition are present, so an unresolved ticket yields an empty reply.
func (css *codesearchServer) tsNavDocumentation(ctx context.Context, req *srpb.ExtendedDocumentationRequest) (*srpb.ExtendedDocumentationReply, error) {
	reply := &srpb.ExtendedDocumentationReply{}
	r, err := css.navReader(ctx)
	if err != nil {
		return nil, err
	}
	defs, err := annotations.Describe(ctx, &navindex.DefLookup{R: r}, req.GetTicket())
	if err != nil {
		return nil, err
	}
	if len(defs) == 0 {
		return reply, nil
	}
	d := defs[0]
	reply.NodeInfo = navNodeInfo(d.Kind)
	reply.Definition = &kxpb.CrossReferencesReply_RelatedAnchor{
		Anchor: &kxpb.Anchor{
			Parent:  ticketForPath(d.Path),
			Span:    navSpan(d.Start, d.End),
			Snippet: d.Signature,
		},
	}
	reply.Docstring = d.Doc
	return reply, nil
}

// navNodeInfo maps a declaration kind to the kythe node facts the
// frontend reads (/kythe/node/kind and /kythe/subkind) to label the hover.
func navNodeInfo(kind string) *kcpb.NodeInfo {
	var nodeKind, subkind string
	switch kind {
	case "func", "method":
		nodeKind = "function"
	case "struct":
		nodeKind, subkind = "record", "struct"
	case "class":
		nodeKind, subkind = "record", "class"
	case "enum":
		nodeKind, subkind = "record", "enum"
	case "type", "interface":
		nodeKind, subkind = "record", "type"
	case "const":
		nodeKind = "constant"
	case "var":
		nodeKind = "variable"
	default:
		nodeKind = "function" // a sensible default so the hover still renders
	}
	facts := map[string][]byte{"/kythe/node/kind": []byte(nodeKind)}
	if subkind != "" {
		facts["/kythe/subkind"] = []byte(subkind)
	}
	return &kcpb.NodeInfo{Facts: facts}
}

// tsNavExtendedXrefs answers a references-panel request from tree-sitter: for
// each ticket it fills the Definitions bucket (the declarations, with their
// signatures as snippets) and the References bucket (every use site across
// files importing the package or belonging to it, with the source line as
// snippet). The override/extends/generates buckets are kythe-semantic and stay
// empty — syntactic nav doesn't recover them.
func (css *codesearchServer) tsNavExtendedXrefs(ctx context.Context, req *srpb.ExtendedXrefsRequest) (*srpb.ExtendedXrefsReply, error) {
	r, err := css.navReader(ctx)
	if err != nil {
		return nil, err
	}
	lk := &navindex.DefLookup{R: r}

	reply := &srpb.ExtendedXrefsReply{}
	for _, ticket := range req.GetTickets() {
		defs, err := annotations.Describe(ctx, lk, ticket)
		if err != nil {
			return nil, err
		}
		for _, d := range defs {
			reply.Definitions = append(reply.Definitions, &kxpb.CrossReferencesReply_RelatedAnchor{
				Anchor: &kxpb.Anchor{
					Parent:  ticketForPath(d.Path),
					Span:    navSpan(d.Start, d.End),
					Snippet: d.Signature,
				},
			})
		}
		refs, err := annotations.FindReferences(ctx, lk, ticket)
		if err != nil {
			return nil, err
		}
		for _, rf := range refs {
			reply.References = append(reply.References, &kxpb.CrossReferencesReply_RelatedAnchor{
				Anchor: &kxpb.Anchor{
					Parent:  ticketForPath(rf.Path),
					Span:    navSpan(rf.Start, rf.End),
					Snippet: rf.Snippet,
				},
			})
		}
	}
	return reply, nil
}

// navReader opens a GitHubFileSchema reader on the configured nav namespace
// within the authenticated group.
func (css *codesearchServer) navReader(ctx context.Context) (*index.Reader, error) {
	ns, err := css.getUserNamespace(ctx, *treeSitterNavNamespace)
	if err != nil {
		return nil, err
	}
	return index.NewReader(ctx, css.db, ns, schema.GitHubFileSchema()), nil
}

const navCorpus = annotations.Scheme + "://buildbuddy?path="

// ticketForPath formats a definition's file ticket the frontend can extract a
// path from (it splits on "?path="). Uses the tree-sitter:// scheme so that
// navigating to a definition re-enters tree-sitter nav for that file.
func ticketForPath(path string) string {
	return navCorpus + path
}

// pathFromTicket extracts the `path` query parameter from a file ticket.
func pathFromTicket(ticket string) string {
	u, err := url.Parse(ticket)
	if err != nil {
		return ""
	}
	return u.Query().Get("path")
}

func navPoint(p annotations.Pos) *kcpb.Point {
	return &kcpb.Point{
		ByteOffset:   int32(p.Byte),
		LineNumber:   int32(p.Line),
		ColumnOffset: int32(p.Col),
	}
}

func navSpan(start, end annotations.Pos) *kcpb.Span {
	return &kcpb.Span{Start: navPoint(start), End: navPoint(end)}
}
