package summaries

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
)

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
	f := filterByKey[strings.ToLower(field)]
	if f == nil {
		return nil, fmt.Errorf("unknown filter %q", field)
	}
	return filterAndRank(f.values(ix, key), prefix, limit), nil
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
			c.health[e.healthName()]++
			for k, v := range e.Labels {
				k = strings.ToLower(k)
				c.labelKeys[k]++
				if v == "" {
					continue
				}
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
