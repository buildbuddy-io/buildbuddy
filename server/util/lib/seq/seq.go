// seq exists to save allocations when iterating over intermediary versions of a
// sequence that is being processed, as well as to save cpu time in the case
// that it turns out that not all of a list was needed, as these functions
// evaluate the sequences they are passed lazily and do not allocate slices for
// intermediary values.
//
// Guarantees:
// - All sequences returned by functions in this library are stateless as long
// as the parameters passed to the functions are stateless.
// - No function in this library will iterate over a passed sequence more than
// once.
package seq

import (
	"fmt"
	"iter"
	"slices"
)

// Sequenceable is a constraint which requires the type it constrains to either
// be a slice or an iter.Seq.
type Sequenceable[E any] interface {
	[]E | iter.Seq[E]
}

// Sequence turns a Sequenceable into an iter.Seq.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless. If the parameter is a
// slice, it is not cloned, so changing the underlying slice will change the
// elements of the returned sequence.
func Sequence[E any, S Sequenceable[E]](s S) iter.Seq[E] {
	if s == nil {
		return EmptySeq[E]
	}
	switch v := any(s).(type) {
	case []E:
		return slices.Values(v)
	case iter.Seq[E]:
		return v
	default:
		// This should be impossible
		panic(fmt.Errorf("seq.Sequence called with invalid type %T. This code should never be reached.", s))
	}
}

// EmptySeq is the (stateless) empty sequence.
func EmptySeq[E any](func(E) bool) {}

// Chain chains two Sequenceable types together into one sequence and returns
// the result.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
// NOTE: as of writing, go's type inference does not work on this function, so
// the element type must be specified as a type parameter. However, if
// https://github.com/golang/go/issues/73527 is ever accepted/addressed, the
// explicit type specification will no longer be needed.
func Chain[E any, S1 Sequenceable[E], S2 Sequenceable[E]](s1 S1, s2 S2) iter.Seq[E] {
	return func(yield func(E) bool) {
		for e := range Sequence[E](s1) {
			if !yield(e) {
				return
			}
		}
		for e := range Sequence[E](s2) {
			if !yield(e) {
				return
			}
		}
	}
}

// Fmap maps a function over the passed sequence, which is to say it returns the
// result of calling the passed function on each of the elements in the passed
// sequence as a sequence.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func Fmap[I any, O any, S Sequenceable[I]](s S, f func(I) O) iter.Seq[O] {
	return func(yield func(O) bool) {
		for e := range Sequence[I](s) {
			if !yield(f(e)) {
				return
			}
		}
	}
}

// Drop drops the first N elements from the passed sequence after N elements. If
// the sequence is fewer than N elements in length, it will return an empty
// sequence. If N is less than one, the sequence is returned unchanged.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
// NOTE: as of writing, go's type inference does not work on this function, so
// the element type must be specified as a type parameter. However, if
// https://github.com/golang/go/issues/73527 is ever accepted/addressed, the
// explicit type specification will no longer be needed.
func Drop[E any, S Sequenceable[E]](s S, n int) iter.Seq[E] {
	if n < 1 {
		return Sequence[E](s)
	}
	return func(yield func(E) bool) {
		i := 0
		for e := range Sequence[E](s) {
			if i < n {
				i++
				continue
			}
			if !yield(e) {
				return
			}
		}
	}
}

// Take returns a sequence consisting only of the first N elements of the passed
// sequence. If the sequence is fewer than N elements in length, it will be
// returned unchanged. If N is less than one, the empty sequence is returned.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
// NOTE: as of writing, go's type inference does not work on this function, so
// the element type must be specified as a type parameter. However, if
// https://github.com/golang/go/issues/73527 is ever accepted/addressed, the
// explicit type specification will no longer be needed.
func Take[E any, S Sequenceable[E]](s S, n int) iter.Seq[E] {
	if n < 1 {
		return EmptySeq[E]
	}
	return func(yield func(E) bool) {
		i := 0
		for e := range Sequence[E](s) {
			if !yield(e) {
				return
			}
			i++
			if i >= n {
				return
			}
		}
	}
}

func setupRepeat[E any, S Sequenceable[E]](s S) (*[]E, iter.Seq[E]) {
	elements := new([]E)
	var sequence iter.Seq[E]
	switch v := any(s).(type) {
	case []E:
		elements = &v
		sequence = slices.Values(v)
	case iter.Seq[E]:
		sequence = Fmap(v, func(e E) E {
			// populates elements while iterating the sequence, if necessary.
			*elements = append(*elements, e)
			return e
		})
	}
	return elements, sequence
}

// Repeat repeats the passed sequence forever. Since sequences can not be
// iterated multiple times, it will allocate a slice (if it is not passed one)
// to store the elements on the first pass so that they can be repeated on
// subsequent passes. If the passed sequence is zero-length, the returned
// sequence will also be zero-length.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
// NOTE: as of writing, go's type inference does not work on this function, so
// the element type must be specified as a type parameter. However, if
// https://github.com/golang/go/issues/73527 is ever accepted/addressed, the
// explicit type specification will no longer be needed.
func Repeat[E any, S Sequenceable[E]](s S) iter.Seq[E] {
	return func(yield func(E) bool) {
		elements, sequence := setupRepeat[E](s)
		for e := range sequence {
			if !yield(e) {
				return
			}
		}
		if len(*elements) == 0 {
			return
		}
		for {
			for _, e := range *elements {
				if !yield(e) {
					return
				}
			}
		}
	}
}

// RepeatN repeats the passed sequence the specified number of times. Since
// sequences can not be iterated multiple times, it will allocate a slice (if it
// is not passed one) to store the elements on the first pass so that it can
// repeat them on subsequent passes if N is greater than one. If N is less than
// one, the empty sequence is returned.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
// NOTE: as of writing, go's type inference does not work on this function, so
// the element type must be specified as a type parameter. However, if
// https://github.com/golang/go/issues/73527 is ever accepted/addressed, the
// explicit type specification will no longer be needed.
func RepeatN[E any, S Sequenceable[E]](s S, n int) iter.Seq[E] {
	switch {
	case n < 1:
		return EmptySeq[E]
	case n == 1:
		return Sequence[E](s)
	default:
		return func(yield func(E) bool) {
			elements, sequence := setupRepeat[E](s)
			for e := range sequence {
				if !yield(e) {
					return
				}
			}
			if len(*elements) == 0 {
				return
			}
			for i := 1; i < n; i++ {
				for _, e := range *elements {
					if !yield(e) {
						return
					}
				}
			}
		}
	}
}

// Sum returns the result of joining all of the elements with the passed
// function acting as a left-associative infix binary operator. It is very
// similar to Accumulate, except that it uses the first value of the sequence as
// the initial value, and thus the return value must be the same type as the
// elements of the sequence. If the sequence is length one, the lone element is
// returned unmodified. If the sequence is empty, the zero-value of the element
// type is returned.
func Sum[E any, S Sequenceable[E]](s S, f func(E, E) E) E {
	var sum *E
	for e := range Sequence[E](s) {
		if sum == nil {
			sum = &e
			continue
		}
		*sum = f(*sum, e)
	}
	if sum == nil {
		return *new(E)
	}
	return *sum
}

// Accumulate accumulates all of the elements in the passed sequence into a
// single value using the passed function and initial value.
func Accumulate[E any, V any, S Sequenceable[E]](s S, init V, f func(V, E) V) V {
	v := init
	for e := range Sequence[E](s) {
		v = f(v, e)
	}
	return v
}

// Any is a special-case of Accumulate that returns true if the passed function
// returns true for any of the elements in the passed sequence. It will
// short-circuit, skipping processing the rest of the list, as soon as it finds
// an element for which the passed function returns true.
func Any[E any, S Sequenceable[E]](s S, f func(E) bool) bool {
	for e := range Sequence[E](s) {
		if f(e) {
			return true
		}
	}
	return false
}

// None is a special-case of Accumulate that returns true if the passed function
// returns false for all of the elements in the passed sequence. It will
// short-circuit, skipping processing the rest of the list, as soon as it finds
// an element for which the passed function returns true.
func None[E any, S Sequenceable[E]](s S, f func(E) bool) bool {
	return !Any(s, f)
}

// All is a special-case of Accumulate that returns true if the passed function
// returns true for all of the elements in the passed sequence. It will
// short-circuit, skipping processing the rest of the list, as soon as it finds
// an element for which the passed function returns false.
func All[E any, S Sequenceable[E]](s S, f func(E) bool) bool {
	return !Any(s, func(e E) bool { return !f(e) })
}

// ComposeFilters takes a list of functions and returns a function that only
// returns true if all of the passed functions return true.
func ComposeFilters[E any](filters ...func(E) bool) func(E) bool {
	return func(e E) bool {
		for _, filter := range filters {
			if !filter(e) {
				return false
			}
		}
		return true
	}
}

// Filter filters out all elements from the passed sequence for which any of the
// passed functions return false.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func Filter[E any, S Sequenceable[E]](s S, filter func(E) bool) iter.Seq[E] {
	if filter == nil {
		return Sequence[E](s)
	}
	return func(yield func(E) bool) {
		for e := range Sequence[E](s) {
			if filter(e) {
				if !yield(e) {
					return
				}
			}
		}
	}
}
