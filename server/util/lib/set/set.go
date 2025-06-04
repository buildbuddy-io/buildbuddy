package set

import (
	"iter"
	"maps"
)

// View is a (read-only) view of a data structure as a set.
type View[E comparable] interface {
	All() iter.Seq[E]
	Contains(E) bool
	Intersection(View[E]) iter.Seq[E]
	Union(View[E]) iter.Seq[E]
	Difference(View[E]) iter.Seq[E]
}

// Nothing is an alias for struct{} for clarity/readability.
type Nothing = struct{}

// Set is just a lightweight wrapper around the standard golang stand-in for the
// set type, map[E]struct{}. It is intended to improve readability and reduce
// code duplication. An empty Set can be made with `make(Set[E])` or
// `make(Set[E], cap)`, just like a normal map.
type Set[E comparable] map[E]Nothing

// Set is just a lightweight wrapper around the standard golang stand-in for the
// set type, map[E]struct{}. It is intended to improve readability and reduce
// code duplication. An empty Set can be made with `make(Set[E])` or
// `make(Set[E], cap)`, just like a normal map.
type mapView[E comparable, V any] map[E]V

// From creates a new Set containing all the provided elements.
func From[E comparable](s ...E) Set[E] {
	set := make(Set[E], len(s))
	for _, e := range s {
		set[e] = Nothing{}
	}
	return set
}

// FromSeq creates a new Set containing all terms in the provided sequence.
func FromSeq[E comparable](s iter.Seq[E]) Set[E] {
	set := make(Set[E])
	for e := range s {
		set[e] = Nothing{}
	}
	return set
}

func AsView[E comparable, V any](m map[E]V) View[E] {
	return mapView[E, V](m)
}

// All returns a sequence of all the elements in this set.
func (s Set[E]) All() iter.Seq[E] {
	return AsView(s).All()
}

// All returns a sequence of all the elements in this set.
func (s mapView[E, V]) All() iter.Seq[E] {
	return maps.Keys(s)
}

// Add adds the provided element to the set if it is not yet a member of the
// set.
func (s Set[E]) Add(e E) {
	s[e] = struct{}{}
}

// Remove removes the provided element from the set if it is currently a
// member of the set.
func (s Set[E]) Remove(e E) {
	delete(s, e)
}

// Contains returns true if the provided element is a member of the set, and
// false if it is not.
func (s Set[E]) Contains(e E) bool {
	return AsView(s).Contains(e)
}

// Contains returns true if the provided element is a member of the set, and
// false if it is not.
func (s mapView[E, V]) Contains(e E) bool {
	_, ok := s[e]
	return ok
}

// Intersection returns the intersection of this set and the passed conjunct.
func (s Set[E]) Intersection(conjunct View[E]) iter.Seq[E] {
	return AsView(s).Intersection(conjunct)
}

// Intersection returns the intersection of this set and the passed conjunct.
func (s mapView[E, V]) Intersection(conjunct View[E]) iter.Seq[E] {
	return func(yield func(E) bool) {
		for e := range s {
			if conjunct.Contains(e) {
				if !yield(e) {
					return
				}
			}
		}
	}
}

// Union returns the union of this set and the passed disjunct.
func (s Set[E]) Union(disjunct View[E]) iter.Seq[E] {
	return AsView(s).Union(disjunct)
}

// Union returns the union of this set and the passed disjunct.
func (s mapView[E, V]) Union(disjunct View[E]) iter.Seq[E] {
	return func(yield func(E) bool) {
		for e := range s {
			if !yield(e) {
				return
			}
		}
		for e := range disjunct.Difference(s) {
			if !yield(e) {
				return
			}
		}
	}
}

// Difference returns this set with all the elements in the subtrahend removed.
func (s Set[E]) Difference(subtrahend View[E]) iter.Seq[E] {
	return AsView(s).Difference(subtrahend)
}

// Difference returns this set with all the elements in the subtrahend removed.
func (s mapView[E, V]) Difference(subtrahend View[E]) iter.Seq[E] {
	return func(yield func(E) bool) {
		for e := range s {
			if subtrahend.Contains(e) {
				continue
			}
			if !yield(e) {
				return
			}
		}
	}
}
