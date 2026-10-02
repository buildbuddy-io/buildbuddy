package summaries

import (
	"cmp"
	"slices"
	"strings"
)

// Query is a parsed search input.
//
// Filters use a "key:value" syntax (kind:, ns:/namespace:, cluster:, label:).
// Any other token containing a colon is a plain term, so image references like
// "redis:7.2" search as expected.
type Query struct {
	Terms     []string
	Kind      string
	Namespace string
	Cluster   string
	Labels    []string // "k=v" (exact) or "k" (presence)
}

// ParseQuery splits a search string into terms and filters.
func ParseQuery(s string) Query {
	q := Query{}
	for tok := range strings.FieldsSeq(strings.ToLower(s)) {
		switch {
		case strings.HasPrefix(tok, "kind:"):
			q.Kind = strings.TrimPrefix(tok, "kind:")
		case strings.HasPrefix(tok, "ns:"):
			q.Namespace = strings.TrimPrefix(tok, "ns:")
		case strings.HasPrefix(tok, "namespace:"):
			q.Namespace = strings.TrimPrefix(tok, "namespace:")
		case strings.HasPrefix(tok, "cluster:"):
			q.Cluster = strings.TrimPrefix(tok, "cluster:")
		case strings.HasPrefix(tok, "label:"):
			if v := strings.TrimPrefix(tok, "label:"); v != "" {
				q.Labels = append(q.Labels, v)
			}
		default:
			q.Terms = append(q.Terms, tok)
		}
	}
	return q
}

// IsEmpty reports whether the query would match everything.
func (q Query) IsEmpty() bool {
	return len(q.Terms) == 0 && q.Kind == "" && q.Namespace == "" && q.Cluster == "" && len(q.Labels) == 0
}

// matchesResourceType checks whether the query filters by resource type and
// the resource type matches. If the resource type doesn't match we can skip
// more datailed matching.
func (q Query) matchesResourceType(r ResourceType) bool {
	if q.Kind != "" && !strings.HasPrefix(strings.ToLower(r.Kind), q.Kind) && !strings.HasPrefix(strings.ToLower(r.Resource), q.Kind) {
		return false
	}
	return q.Cluster == "" || strings.HasPrefix(strings.ToLower(r.Cluster), q.Cluster)
}

func (q Query) matches(e *Entry) bool {
	if q.Namespace != "" && !strings.HasPrefix(strings.ToLower(e.Namespace), q.Namespace) {
		return false
	}
	for _, l := range q.Labels {
		// Labels match case-insensitively, since values such as "Helm" are
		// ordinary.
		k, v, hasValue := strings.Cut(l, "=")
		found := false
		for lk, lv := range e.Labels {
			if strings.EqualFold(lk, k) && (!hasValue || strings.EqualFold(lv, v)) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for _, t := range q.Terms {
		if !strings.Contains(e.blob, t) {
			return false
		}
	}
	return true
}

// score ranks a matched entry by how well each term matches its name: the
// whole name, a whole word of it ("app" in "buildbuddy-app-7d9f"), a prefix,
// a substring, or not at all (the term hit a label, an image, ...).
func (q Query) score(e *Entry) int {
	s := kindWeight(e.Kind)
	name := strings.ToLower(e.Name)
	for _, t := range q.Terms {
		switch {
		case name == t:
			s += 100
		case hasWord(name, t):
			s += 80
		case strings.HasPrefix(name, t):
			s += 60
		case strings.Contains(name, t):
			s += 40
		default:
			s += 10
		}
	}
	return s
}

// hasWord reports whether term is one of name's dash- or dot-separated words.
func hasWord(name, term string) bool {
	words := strings.FieldsFunc(name, func(r rune) bool { return r == '-' || r == '.' })
	return slices.Contains(words, term)
}

// kindWeight breaks ties between kinds based on how likely it's a resource
// someone is looking for.
func kindWeight(kind string) int {
	switch kind {
	case "Pod", "Deployment":
		return 9
	case "Service", "Node", "StatefulSet":
		return 8
	case "DaemonSet", "Namespace", "Ingress":
		return 7
	case "Job", "CronJob":
		return 6
	case "ConfigMap", "Secret":
		return 4
	case "ReplicaSet":
		return 3
	}
	return 2
}

// SearchGroup are the results for resources of a single kind. Group tells
// apart unrelated kinds that share a name across API groups.
type SearchGroup struct {
	Group string
	Kind  string
	// Total number of matched resources. Items may be capped to a subset of all resources.
	Total int
	Items []*Entry
}

// SearchResult is a ranked, kind-grouped set of matches.
type SearchResult struct {
	Groups []SearchGroup
	Total  int
}

type scored struct {
	e *Entry
	s int
}

const DefaultGroupLimit = 15

// Search runs a query over every store and groups matches by group and kind.
func (ix *Index) Search(rawQuery string, groupLimit int) *SearchResult {
	if groupLimit <= 0 {
		groupLimit = DefaultGroupLimit
	}
	res := &SearchResult{Groups: []SearchGroup{}}
	q := ParseQuery(rawQuery)
	if q.IsEmpty() {
		return res
	}

	type groupKind struct{ group, kind string }
	byKind := map[groupKind][]scored{}
	for _, store := range ix.snapshot() {
		if !q.matchesResourceType(store.res) {
			continue
		}
		gk := groupKind{store.res.Group, store.res.Kind}
		store.scan(func(e *Entry) {
			if q.matches(e) {
				byKind[gk] = append(byKind[gk], scored{e, q.score(e)})
			}
		})
	}

	type rankedGroup struct {
		g    SearchGroup
		best int
	}
	var groups []rankedGroup
	for gk, matches := range byKind {
		slices.SortFunc(matches, func(a, b scored) int {
			return cmp.Or(
				cmp.Compare(b.s, a.s), // highest score first
				cmp.Compare(a.e.Name, b.e.Name),
				cmp.Compare(a.e.Namespace, b.e.Namespace),
				cmp.Compare(a.e.Cluster, b.e.Cluster),
			)
		})
		g := SearchGroup{Group: gk.group, Kind: gk.kind, Total: len(matches)}
		for _, m := range matches[:min(groupLimit, len(matches))] {
			g.Items = append(g.Items, m.e)
		}
		groups = append(groups, rankedGroup{g: g, best: matches[0].s})
		res.Total += len(matches)
	}

	// A group's rank is its highest scoring resource.
	// This allows prioritization of groups with exact-name matches.
	slices.SortFunc(groups, func(a, b rankedGroup) int {
		return cmp.Or(
			cmp.Compare(b.best, a.best),
			cmp.Compare(a.g.Kind, b.g.Kind),
			cmp.Compare(a.g.Group, b.g.Group),
		)
	})
	for _, rg := range groups {
		res.Groups = append(res.Groups, rg.g)
	}
	return res
}
