package summaries

import "strings"

// filter describes a field filter supported by search.
type filter struct {
	// key is the field key, without the colon (e.g. "kind", "health", etc)
	key string
	// hint is the text description shown in the auto-complete for this field.
	// An empty hint hides the type from auto-complete (for aliases).
	hint string

	// parse applies the value to a query.
	parse func(q *Query, value string)

	// values returns the possible keys/values for a given field, used by
	// auto-complete. For key-value fields (such as "label") the labelKey
	// determines whether field keys or field values are returned.
	values func(ix *Index, labelKey string) map[string]int
}

var (
	setNamespace = func(q *Query, v string) { q.Namespace = v }
	namespaces   = func(ix *Index, _ string) map[string]int { return ix.catalog().namespaces }
)

var filters = []filter{
	{
		key:    "kind",
		hint:   "a resource type, e.g. kind:pod",
		parse:  func(q *Query, v string) { q.Kind = v },
		values: func(ix *Index, _ string) map[string]int { return ix.kindCounts() },
	},
	{key: "ns", hint: "a namespace prefix, e.g. ns:prod", parse: setNamespace, values: namespaces},
	// Alias for "ns".
	{key: "namespace", parse: setNamespace, values: namespaces},
	{
		key:  "label",
		hint: "a label, or label=value, e.g. label:app=web",
		parse: func(q *Query, v string) {
			if v != "" {
				q.Labels = append(q.Labels, v)
			}
		},
		values: func(ix *Index, labelKey string) map[string]int {
			c := ix.catalog()
			if labelKey == "" {
				return c.labelKeys
			}
			return c.labelValues[strings.ToLower(labelKey)]
		},
	},
	{
		key:    "health",
		hint:   "ok, warn, bad or unknown",
		parse:  func(q *Query, v string) { q.Health = v },
		values: func(ix *Index, _ string) map[string]int { return ix.catalog().health },
	},
	{
		key:    "cluster",
		hint:   "a cluster name prefix",
		parse:  func(q *Query, v string) { q.Cluster = v },
		values: func(ix *Index, _ string) map[string]int { return ix.catalog().clusters },
	},
}

var filterByKey = func() map[string]*filter {
	m := map[string]*filter{}
	for i := range filters {
		m[filters[i].key] = &filters[i]
	}
	return m
}()

// FilterKey is a key that a search can match on, with a description shown
// in the UI.
type FilterKey struct {
	Key, Hint string
}

// FilterKeys are the filters shown in the auto-complete popup.
var FilterKeys = func() []FilterKey {
	var keys []FilterKey
	for _, f := range filters {
		if f.hint != "" {
			keys = append(keys, FilterKey{f.key, f.hint})
		}
	}
	return keys
}()
