// Navigation request handlers. These answer the code browser's four nav
// requests — decorations, go-to-definition, hover documentation, and the
// references panel — from tree-sitter annotations over the given index
// reader, translating results into the kythe reply protos the frontend
// already renders. This is the kythe-free, syntactic nav tier: it needs only
// indexed file contents and the existing symbols/import_id posting lists.

package nav

import (
	"context"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	srpb "github.com/buildbuddy-io/buildbuddy/proto/search"
	kcpb "kythe.io/kythe/proto/common_go_proto"
	kxpb "kythe.io/kythe/proto/xref_go_proto"
)

// Decorations answers a decorations request: it loads the viewed file from
// the index and returns a clickable reference for every resolvable identifier
// occurrence. Unknown files (or unsupported languages, which produce no refs)
// yield an empty reply, never an error, so the frontend simply shows no
// decorations.
func Decorations(ctx context.Context, r *index.Reader, req *kxpb.DecorationsRequest) (*kxpb.DecorationsReply, error) {
	reply := &kxpb.DecorationsReply{Location: req.GetLocation()}

	owner, repo, path := parseFileTicket(req.GetLocation().GetTicket())
	if path == "" {
		return reply, nil
	}
	f, ok := FindFile(ctx, r, owner, repo, path)
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
			Span:         span(rf.Start, rf.End),
		})
	}
	return reply, nil
}

// CrossReferences resolves each requested target ticket to its definition
// site(s) via the index's symbols/import_id posting lists, returning kythe
// CrossReferenceSets the frontend navigates to. Tickets the nav layer did not
// mint resolve to nothing and are simply omitted.
func CrossReferences(ctx context.Context, r *index.Reader, req *kxpb.CrossReferencesRequest) (*kxpb.CrossReferencesReply, error) {
	lk := &DefLookup{R: r}

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
					Parent: fileTicket(loc.Owner, loc.Repo, loc.Path),
					Span:   span(loc.Start, loc.End),
				},
			})
		}
		reply.CrossReferences[ticket] = set
	}
	return reply, nil
}

// Documentation answers a hover request: it resolves the ticket to its
// definition and returns the declaration's kind, signature, and leading doc
// comment. The frontend renders nothing unless both node info and a
// definition are present, so an unresolved ticket yields an empty reply.
func Documentation(ctx context.Context, r *index.Reader, req *srpb.ExtendedDocumentationRequest) (*srpb.ExtendedDocumentationReply, error) {
	reply := &srpb.ExtendedDocumentationReply{}
	defs, err := annotations.Describe(ctx, &DefLookup{R: r}, req.GetTicket())
	if err != nil {
		return nil, err
	}
	if len(defs) == 0 {
		return reply, nil
	}
	d := defs[0]
	reply.NodeInfo = nodeInfo(d.Kind)
	reply.Definition = &kxpb.CrossReferencesReply_RelatedAnchor{
		Anchor: &kxpb.Anchor{
			Parent:  fileTicket(d.Owner, d.Repo, d.Path),
			Span:    span(d.Start, d.End),
			Snippet: d.Signature,
		},
	}
	reply.Docstring = d.Doc
	return reply, nil
}

// ExtendedXrefs answers a references-panel request: for each ticket it fills
// the Definitions bucket (the declarations, with their signatures as
// snippets) and the References bucket (every use site across files importing
// the package or belonging to it, with the source line as snippet). The
// override/extends/generates buckets are kythe-semantic and stay empty —
// syntactic nav doesn't recover them.
func ExtendedXrefs(ctx context.Context, r *index.Reader, req *srpb.ExtendedXrefsRequest) (*srpb.ExtendedXrefsReply, error) {
	lk := &DefLookup{R: r}

	reply := &srpb.ExtendedXrefsReply{}
	for _, ticket := range req.GetTickets() {
		defs, err := annotations.Describe(ctx, lk, ticket)
		if err != nil {
			return nil, err
		}
		for _, d := range defs {
			reply.Definitions = append(reply.Definitions, &kxpb.CrossReferencesReply_RelatedAnchor{
				Anchor: &kxpb.Anchor{
					Parent:  fileTicket(d.Owner, d.Repo, d.Path),
					Span:    span(d.Start, d.End),
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
					Parent:  fileTicket(rf.Owner, rf.Repo, rf.Path),
					Span:    span(rf.Start, rf.End),
					Snippet: rf.Snippet,
				},
			})
		}
	}
	return reply, nil
}

// nodeInfo maps a declaration kind to the kythe node facts the frontend reads
// (/kythe/node/kind and /kythe/subkind) to label the hover.
func nodeInfo(kind string) *kcpb.NodeInfo {
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

// fileTicket formats a file ticket: the repository (owner/repo authority) and
// the repo-relative path as the query, e.g.
// tree-sitter://buildbuddy-io/buildbuddy?path=a/b.go.
func fileTicket(owner, repo, path string) string {
	return annotations.Scheme + "://" + owner + "/" + repo + "?path=" + path
}

// parseFileTicket extracts the owner, repo, and repo-relative path from a
// decorations file ticket minted by the frontend:
// tree-sitter://<owner>/<repo>?path=<path>. Owner and repo identify the exact
// indexed document (see FindFile); a bad ticket yields empty strings.
func parseFileTicket(ticket string) (owner, repo, path string) {
	u, err := url.Parse(ticket)
	if err != nil {
		return "", "", ""
	}
	return u.Host, strings.TrimPrefix(u.Path, "/"), u.Query().Get("path")
}

func point(p annotations.Pos) *kcpb.Point {
	return &kcpb.Point{
		ByteOffset:   int32(p.Byte),
		LineNumber:   int32(p.Line),
		ColumnOffset: int32(p.Col),
	}
}

func span(start, end annotations.Pos) *kcpb.Span {
	return &kcpb.Span{Start: point(start), End: point(end)}
}
