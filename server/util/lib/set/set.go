package set

import (
	"cmp"
	"iter"
	"maps"
	"slices"
)

// View is a (read-only) view of a data structure as a set.
type View[E comparable] interface {
	All() iter.Seq[E]
	Contains(E) bool
	Len() int
}

// nothing is an alias for struct{} for clarity/readability.
type nothing = struct{}

// Set is just a lightweight wrapper around the standard golang stand-in for the
// set type, map[E]struct{}. It is intended to improve readability and reduce
// code duplication. An empty Set can be made with `make(Set[E])` or
// `make(Set[E], cap)`, just like a normal map.
type Set[E comparable] map[E]nothing

// mapView is a view of a `map[E]V` as a `View[E] `.
type mapView[E comparable, V any] map[E]V

// From creates a new Set containing all the provided elements.
func From[E comparable](s ...E) Set[E] {
	set := make(Set[E], len(s))
	set.AddSeq(slices.Values(s))
	return set
}

// FromSeq creates a new Set containing all terms in the provided sequence.
func FromSeq[E comparable](s iter.Seq[E]) Set[E] {
	set := make(Set[E])
	set.AddSeq(s)
	return set
}

// KeyView returns a View of the keys in the provided map as a set.
func KeyView[E comparable, V any](m map[E]V) View[E] {
	return mapView[E, V](m)
}

// All returns a sequence of all the elements in this set.
func (s Set[E]) All() iter.Seq[E] {
	return KeyView(s).All()
}

// All returns a sequence of all the elements in this set.
func (s mapView[E, V]) All() iter.Seq[E] {
	return maps.Keys(s)
}

// Add adds the provided element to the set if it is not yet a member of the
// set.
func (s Set[E]) Add(e E) {
	s[e] = nothing{}
}

// AddSeq takes a sequence of elements and, for each element, adds it if it is
// not yet a member of the set.
func (s Set[E]) AddSeq(toAdd iter.Seq[E]) {
	for e := range toAdd {
		s.Add(e)
	}
}

// Remove removes the provided element from the set if it is currently a
// member of the set.
func (s Set[E]) Remove(e E) {
	delete(s, e)
}

// RemoveSeq takes a sequence of elements and, for each element, removes it if
// it is currently a member of the set.
func (s Set[E]) RemoveSeq(toRemove iter.Seq[E]) {
	for e := range toRemove {
		s.Remove(e)
	}
}

// Contains returns true if the provided element is a member of the set, and
// false if it is not.
func (s Set[E]) Contains(e E) bool {
	return KeyView(s).Contains(e)
}

// Contains returns true if the provided element is a member of the set, and
// false if it is not.
func (s mapView[E, V]) Contains(e E) bool {
	_, ok := s[e]
	return ok
}

// Len returns the number of elements in the set.
func (s Set[E]) Len() int {
	return len(s)
}

// Len returns the number of elements in the set.
func (s mapView[E, V]) Len() int {
	return len(s)
}

// Intersection returns the intersection of the passed conjuncts.
func Intersection[V View[E], E comparable](conjuncts ...V) iter.Seq[E] {
	if len(conjuncts) == 0 {
		return slices.Values[[]E](nil)
	}
	if len(conjuncts) == 1 {
		return conjuncts[0].All()
	}
	return func(yield func(E) bool) {
		// intersect with the smallest set.
		smallest := slices.MinFunc(conjuncts, func(a V, b V) int {
			return cmp.Compare(a.Len(), b.Len())
		})
		for e := range smallest.All() {
			intersects := true
			for _, conjunct := range conjuncts {
				if !conjunct.Contains(e) {
					intersects = false
					break
				}
			}
			if !intersects {
				continue
			}
			if !yield(e) {
				return
			}
		}
	}
}

// Union returns the union of the passed disjuncts.
func Union[V View[E], E comparable](disjuncts ...V) iter.Seq[E] {
	if len(disjuncts) == 0 {
		return slices.Values[[]E](nil)
	}
	if len(disjuncts) == 1 {
		return disjuncts[0].All()
	}
	return func(yield func(E) bool) {
		if len(disjuncts) == 0 {
			return
		} else if len(disjuncts) == 1 {
			for e := range disjuncts[0].All() {
				if !yield(e) {
					return
				}
			}
			return
		}
		disjuncts := slices.Clone(disjuncts)
		// for len(disjuncts) > 2, it's more efficient to perform the union in
		// descending order of set size.
		slices.SortFunc(disjuncts, func(a V, b V) int {
			return cmp.Compare(b.Len(), a.Len())
		})
		for i, disjunct := range disjuncts {
			for e := range disjunct.All() {
				newElement := true
				for _, d := range disjuncts[0:i] {
					if d.Contains(e) {
						newElement = false
						break
					}
				}
				if !newElement {
					continue
				}
				if !yield(e) {
					return
				}
			}
		}
	}
}

// Difference returns the difference between the passed minuend and the union of
// the passed subtrahends.
func Difference[V View[E], E comparable](minuend View[E], subtrahends ...V) iter.Seq[E] {
	if len(subtrahends) == 0 {
		return minuend.All()
	}
	return func(yield func(E) bool) {
		for e := range minuend.All() {
			keep := true
			for _, subtrahend := range subtrahends {
				if subtrahend.Contains(e) {
					keep = false
					break
				}
			}
			if !keep {
				continue
			}
			if !yield(e) {
				return
			}
		}
	}
}
