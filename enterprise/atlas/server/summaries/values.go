package summaries

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
)

// FilterKey is a key that a search can match on, with a description shown
// in the UI.
type FilterKey struct {
	Key, Hint string
}

var FilterKeys = []FilterKey{
	{"kind", "a resource type, e.g. kind:pod"},
	{"ns", "a namespace prefix, e.g. ns:prod"},
	{"label", "a label, or label=value, e.g. label:app=web"},
	{"health", "ok, warn, bad or unknown"},
	{"cluster", "a cluster name prefix"},
}

type FilterValue struct {
	Value string
	Count int
}

const (
	defaultValuesLimit = 50
	maxValuesLimit     = 1000
)

// GetFilterValues lists possible values for the given search field.
// The field must be a known search field (e.g. "kind", "health", etc). For
// fields that reference key-value pairs (e.g. "label"), the key value specifies
// whether keys or values are returned. If key is specified, possible values for
// that key are returned, otherwise the keys themselves are returned.
func (ix *Index) GetFilterValues(field, key, prefix string, limit int) ([]FilterValue, error) {
	if limit <= 0 {
		limit = defaultValuesLimit
	}
	limit = min(limit, maxValuesLimit)
	var counts map[string]int
	switch strings.ToLower(field) {
	case "kind":
		counts = ix.kindCounts()
	case "ns", "namespace":
		counts = ix.catalog().namespaces
	case "cluster":
		counts = ix.catalog().clusters
	case "health":
		counts = ix.catalog().health
	case "label":
		c := ix.catalog()
		if key == "" {
			counts = c.labelKeys
		} else {
			counts = c.labelValues[c.labelKey(key)]
		}
	default:
		return nil, fmt.Errorf("unknown filter %q", field)
	}
	return filterAndRank(counts, prefix, limit), nil
}

// filterAndRank keeps the values starting with prefix, most common first.
func filterAndRank(counts map[string]int, prefix string, limit int) []FilterValue {
	prefix = strings.ToLower(prefix)
	var out []FilterValue
	for v, n := range counts {
		if strings.HasPrefix(strings.ToLower(v), prefix) {
			out = append(out, FilterValue{Value: v, Count: n})
		}
	}
	slices.SortFunc(out, func(a, b FilterValue) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Value, b.Value))
	})
	return out[:min(limit, len(out))]
}

func (ix *Index) kindCounts() map[string]int {
	counts := map[string]int{}
	for _, s := range ix.snapshot() {
		counts[strings.ToLower(s.res.Kind)] += s.Count()
	}
	return counts
}

// catalog is an in-memory cache of search field values.
type catalog struct {
	built                                   time.Time
	clusters, namespaces, health, labelKeys map[string]int
	labelValues                             map[string]map[string]int
}

const catalogMaxAge = 5 * time.Second

func (ix *Index) catalog() *catalog {
	ix.catalogMu.Lock()
	defer ix.catalogMu.Unlock()
	if ix.cat != nil && time.Since(ix.cat.built) < catalogMaxAge {
		return ix.cat
	}
	c := &catalog{
		built:       time.Now(),
		clusters:    map[string]int{},
		namespaces:  map[string]int{},
		health:      map[string]int{},
		labelKeys:   map[string]int{},
		labelValues: map[string]map[string]int{},
	}
	for _, s := range ix.snapshot() {
		s.scan(func(e *Entry) {
			c.clusters[e.Cluster]++
			if e.Namespace != "" {
				c.namespaces[e.Namespace]++
			}
			c.health[cmp.Or(string(e.Health), "unknown")]++
			for k, v := range e.Labels {
				c.labelKeys[k]++
				vals := c.labelValues[k]
				if vals == nil {
					vals = map[string]int{}
					c.labelValues[k] = vals
				}
				vals[v]++
			}
		})
	}
	ix.cat = c
	return c
}

// labelKey returns the key's own spelling for one typed in any case.
func (c *catalog) labelKey(typed string) string {
	for k := range c.labelKeys {
		if strings.EqualFold(k, typed) {
			return k
		}
	}
	return ""
}
