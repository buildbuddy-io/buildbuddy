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

type ElementPair[T1 any, T2 any] struct {
	First  *T1
	Second *T2
}

func PairPredicate[E1 any, E2 any](f func(E1, E2) bool) func(ElementPair[E1, E2]) bool {
	return func(e ElementPair[E1, E2]) bool {
		return f(*e.First, *e.Second)
	}
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

// EmptySeq is the (stateless) empty sequence.
func EmptySeq2[E1 any, E2 any](func(E1, E2) bool) {}

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

// Chain2 chains two Sequenceable types together into one sequence and returns
// the result.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func Chain2[E1 any, E2 any](s1 iter.Seq2[E1, E2], s2 iter.Seq2[E1, E2]) iter.Seq2[E1, E2] {
	return Unpair(
		Chain[ElementPair[E1, E2]](
			Pair(s1),
			Pair(s2),
		),
	)
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

// Fmap2 maps a function over the passed sequence, which is to say it returns the
// result of calling the passed function on each of the elements in the passed
// sequence as a sequence.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func Fmap2[I1 any, I2 any, O1 any, O2 any](s iter.Seq2[I1, I2], f func(I1, I2) (O1, O2)) iter.Seq2[O1, O2] {
	return func(yield func(O1, O2) bool) {
		for e1, e2 := range s {
			if !yield(f(e1, e2)) {
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
		drop := true
		i := 0
		for e := range Sequence[E](s) {
			if drop = drop && i < n; drop {
				i++
				continue
			}
			if !yield(e) {
				return
			}
		}
	}
}

// Drop2 drops the first N elements from the passed sequence after N elements. If
// the sequence is fewer than N elements in length, it will return an empty
// sequence. If N is less than one, the sequence is returned unchanged.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func Drop2[E1 any, E2 any](s iter.Seq2[E1, E2], n int) iter.Seq2[E1, E2] {
	return Unpair(
		Drop[ElementPair[E1, E2]](
			Pair(s),
			n,
		),
	)
}

// DropWhile drops the elements from the passed sequence until it encounters an
// element for which the passed function returns false, and returns the rest of
// the sequence, including that element.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func DropWhile[E any, S Sequenceable[E]](s S, f func(E) bool) iter.Seq[E] {
	return func(yield func(E) bool) {
		drop := true
		for e := range Sequence[E](s) {
			if drop = drop && f(e); drop {
				continue
			}
			if !yield(e) {
				return
			}
		}
	}
}

// DropWhile2 drops the elements from the passed sequence until it encounters an
// element for which the passed function returns false, and returns the rest of
// the sequence, including that element.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func DropWhile2[E1 any, E2 any](s iter.Seq2[E1, E2], f func(E1, E2) bool) iter.Seq2[E1, E2] {
	return Unpair(
		DropWhile(
			Pair(s),
			PairPredicate(f),
		),
	)
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
			if i >= n {
				return
			}
			if !yield(e) {
				return
			}
			i++
		}
	}
}

// Take2 returns a sequence consisting only of the first N elements of the passed
// sequence. If the sequence is fewer than N elements in length, it will be
// returned unchanged. If N is less than one, the empty sequence is returned.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func Take2[E1 any, E2 any](s iter.Seq2[E1, E2], n int) iter.Seq2[E1, E2] {
	return Unpair(
		Take[ElementPair[E1, E2]](
			Pair(s),
			n,
		),
	)
}

// TakeWhile returns a sequence consisting of the elements of the passed
// sequence so long as the passed function returns true for those elements. It
// ends as soon as it encounters an element for which the function returns
// false, excluding that element and all following it.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func TakeWhile[E any, S Sequenceable[E]](s S, f func(E) bool) iter.Seq[E] {
	return func(yield func(E) bool) {
		for e := range Sequence[E](s) {
			if !f(e) {
				return
			}
			if !yield(e) {
				return
			}
		}
	}
}

// TakeWhile2 returns a sequence consisting of the elements of the passed
// sequence so long as the passed function returns true for those elements. It
// ends as soon as it encounters an element for which the function returns
// false, excluding that element and all following it.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func TakeWhile2[E1 any, E2 any](s iter.Seq2[E1, E2], f func(E1, E2) bool) iter.Seq2[E1, E2] {
	return Unpair(
		TakeWhile(
			Pair(s),
			PairPredicate(f),
		),
	)
}

func setupRepeat[E any, S Sequenceable[E]](s S) (iter.Seq[*E], iter.Seq[E]) {
	switch v := any(s).(type) {
	case []E:
		return Fmap(v, func(e E) *E { return &e }), slices.Values(v)
	case iter.Seq[E]:
		elements := new([]*E)
		repeater := func(yield func(*E) bool) {
			for _, e := range *elements {
				if !yield(e) {
					return
				}
			}
		}
		sequence := Fmap(v, func(e E) E {
			// populates elements while iterating the sequence, if necessary.
			*elements = append(*elements, &e)
			return e
		})
		return repeater, sequence
	}
	panic("A Sequenceable was neither a slice nor an iter.Seq. This should be impossible")
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
		repeater, sequence := setupRepeat[E](s)
		empty := true
		for e := range sequence {
			empty = false
			if !yield(e) {
				return
			}
		}
		if empty {
			return
		}
		for {
			for e := range repeater {
				if !yield(*e) {
					return
				}
			}
		}
	}
}

// Repeat2 repeats the passed sequence forever. Since sequences can not be
// iterated multiple times, it will allocate a slice (if it is not passed one)
// to store the elements on the first pass so that they can be repeated on
// subsequent passes. If the passed sequence is zero-length, the returned
// sequence will also be zero-length.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func Repeat2[E1 any, E2 any](s iter.Seq2[E1, E2]) iter.Seq2[E1, E2] {
	return Unpair(
		Repeat[ElementPair[E1, E2]](
			Pair(s),
		),
	)
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
			repeater, sequence := setupRepeat[E](s)
			empty := true
			for e := range sequence {
				empty = false
				if !yield(e) {
					return
				}
			}
			if empty {
				return
			}
			for i := 1; i < n; i++ {
				for e := range repeater {
					if !yield(*e) {
						return
					}
				}
			}
		}
	}
}

// RepeatN2 repeats the passed sequence the specified number of times. Since
// sequences can not be iterated multiple times, it will allocate a slice (if it
// is not passed one) to store the elements on the first pass so that it can
// repeat them on subsequent passes if N is greater than one. If N is less than
// one, the empty sequence is returned.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func RepeatN2[E1 any, E2 any](s iter.Seq2[E1, E2], n int) iter.Seq2[E1, E2] {
	return Unpair(
		RepeatN[ElementPair[E1, E2]](
			Pair(s),
			n,
		),
	)
}

// Sum returns the result of joining all of the elements with the passed
// function acting as a left-associative infix binary operator. It is very
// similar to Accumulate, except that it uses the first value of the sequence as
// the initial value, and thus the return value must be the same type as the
// elements of the sequence. If the sequence is length one, the lone element is
// returned unmodified. If the sequence is empty, the zero-value of the element
// type is returned.
func Sum[E any, S Sequenceable[E]](s S, f func(E, E) E) E {
	var sum E
	uninitialized := true
	for e := range Sequence[E](s) {
		if uninitialized {
			sum = e
			uninitialized = false
			continue
		}
		sum = f(sum, e)
	}
	return sum
}

// Sum2 returns the result of joining all of the elements with the passed
// function acting as a left-associative infix binary operator. It is very
// similar to Accumulate, except that it uses the first value of the sequence as
// the initial value, and thus the return value must be the same type as the
// elements of the sequence. If the sequence is length one, the lone element is
// returned unmodified. If the sequence is empty, the zero-value of the element
// type is returned.
func Sum2[E1 any, E2 any](s iter.Seq2[E1, E2], f1 func(E1, E1) E1, f2 func(E2, E2) E2) (E1, E2) {
	var sum1 E1
	var sum2 E2
	uninitialized := true
	for e1, e2 := range s {
		if uninitialized {
			sum1 = e1
			sum2 = e2
			uninitialized = false
			continue
		} else {
			if f1 != nil {
				sum1 = f1(sum1, e1)
			}
			if f2 != nil {
				sum2 = f2(sum2, e2)
			}
		}
	}
	return sum1, sum2
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

// Accumulate2 accumulates all of the elements in the passed sequence into a
// single value using the passed function and initial value.
func Accumulate2[E1 any, E2 any, V any](s iter.Seq2[E1, E2], init V, f func(V, E1, E2) V) V {
	return Accumulate(
		Pair(s),
		init,
		func(v V, e ElementPair[E1, E2]) V {
			return f(v, *e.First, *e.Second)
		},
	)
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

// Any2 is a special-case of Accumulate2 that returns true if the passed function
// returns true for any of the elements in the passed sequence. It will
// short-circuit, skipping processing the rest of the list, as soon as it finds
// an element for which the passed function returns true.
func Any2[E1 any, E2 any](s iter.Seq2[E1, E2], f func(E1, E2) bool) bool {
	return Any(Pair(s), PairPredicate(f))
}

// None is a special-case of Accumulate that returns true if the passed function
// returns false for all of the elements in the passed sequence. It will
// short-circuit, skipping processing the rest of the list, as soon as it finds
// an element for which the passed function returns true.
func None[E any, S Sequenceable[E]](s S, f func(E) bool) bool {
	return !Any(s, f)
}

// None2 is a special-case of Accumulate2 that returns true if the passed function
// returns false for all of the elements in the passed sequence. It will
// short-circuit, skipping processing the rest of the list, as soon as it finds
// an element for which the passed function returns true.
func None2[E1 any, E2 any](s iter.Seq2[E1, E2], f func(E1, E2) bool) bool {
	return !Any2(s, f)
}

// All is a special-case of Accumulate that returns true if the passed function
// returns true for all of the elements in the passed sequence. It will
// short-circuit, skipping processing the rest of the list, as soon as it finds
// an element for which the passed function returns false.
func All[E any, S Sequenceable[E]](s S, f func(E) bool) bool {
	return !Any(s, func(e E) bool { return !f(e) })
}

// All2 is a special-case of Accumulate2 that returns true if the passed function
// returns true for all of the elements in the passed sequence. It will
// short-circuit, skipping processing the rest of the list, as soon as it finds
// an element for which the passed function returns false.
func All2[E1 any, E2 any](s iter.Seq2[E1, E2], f func(E1, E2) bool) bool {
	return !Any2(s, func(e1 E1, e2 E2) bool { return !f(e1, e2) })
}

// ComposeFilters takes a list of functions and returns a function that only
// returns true if all of the passed functions return true.
func ComposeFilters[E any](filters ...func(E) bool) func(E) bool {
	return func(e E) bool {
		for _, filter := range filters {
			if filter == nil {
				continue
			}
			if !filter(e) {
				return false
			}
		}
		return true
	}
}

// ComposeFilters2 takes a list of functions and returns a function that only
// returns true if all of the passed functions return true.
func ComposeFilters2[E1 any, E2 any](filters ...func(E1, E2) bool) func(E1, E2) bool {
	return func(e1 E1, e2 E2) bool {
		for _, filter := range filters {
			if filter == nil {
				continue
			}
			if !filter(e1, e2) {
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

// Filter2 filters out all elements from the passed sequence for which any of the
// passed functions return false.
//
// As with all sequences returned by this library, so long as the parameters are
// stateless, the returned sequence will be stateless.
func Filter2[E1 any, E2 any](s iter.Seq2[E1, E2], filter func(E1, E2) bool) iter.Seq2[E1, E2] {
	return Unpair(
		Filter(
			Pair(s),
			PairPredicate(filter),
		),
	)
}

func Equal[E comparable, S1 Sequenceable[E], S2 Sequenceable[E]](s1 S1, s2 S2) bool {
	return All2(zipLongest[E, E](s1, s2, nil, nil), func(e1 *E, e2 *E) bool {
		return e1 != nil && e2 != nil && *e1 == *e2
	})
}

func Equal2[E1 comparable, E2 comparable](s1 iter.Seq2[E1, E2], s2 iter.Seq2[E1, E2]) bool {
	return All2(
		zipLongest[ElementPair[E1, E2], ElementPair[E1, E2]](Pair(s1), Pair(s2), nil, nil),
		func(e1 *ElementPair[E1, E2], e2 *ElementPair[E1, E2]) bool {
			return e1 != nil && e2 != nil && *(e1.First) == *(e2.First) && *(e1.Second) == *(e2.Second)
		})
}

func zipSliceSlice[E1 any, E2 any](s1 []E1, s2 []E2, fill1 *E1, fill2 *E2) iter.Seq2[*E1, *E2] {
	return func(yield func(*E1, *E2) bool) {
		for i := 0; ; i++ {
			switch {
			case i < len(s1) && i < len(s2):
				if !yield(&s1[i], &s2[i]) {
					return
				}
			case i < len(s1):
				if !yield(&s1[i], fill2) {
					return
				}
			case i < len(s2):
				if !yield(fill1, &s2[i]) {
					return
				}
			default:
				return
			}
		}
	}
}

func zipSliceSeq[E1 any, E2 any](s1 []E1, s2 iter.Seq[E2], fill1 *E1, fill2 *E2) iter.Seq2[*E1, *E2] {
	return func(yield func(*E1, *E2) bool) {
		i := 0
		for e2 := range s2 {
			if i < len(s1) {
				if !yield(&s1[i], &e2) {
					return
				}
			} else {
				if !yield(fill1, &e2) {
					return
				}
			}
			i++
		}
		for ; i < len(s1); i++ {
			if !yield(&s1[i], fill2) {
				return
			}
		}
	}
}

func zipSeqSlice[E1 any, E2 any](s1 iter.Seq[E1], s2 []E2, fill1 *E1, fill2 *E2) iter.Seq2[*E1, *E2] {
	return func(yield func(*E1, *E2) bool) {
		i := 0
		for e1 := range s1 {
			if i < len(s2) {
				if !yield(&e1, &s2[i]) {
					return
				}
			} else {
				if !yield(&e1, fill2) {
					return
				}
			}
			i++
		}
		for ; i < len(s2); i++ {
			if !yield(fill1, &s2[i]) {
				return
			}
		}
	}
}

func zipSeqSeq[E1 any, E2 any](s1 iter.Seq[E1], s2 iter.Seq[E2], fill1 *E1, fill2 *E2) iter.Seq2[*E1, *E2] {
	return func(yield func(*E1, *E2) bool) {
		next1, stop1 := iter.Pull(Sequence[E1](s1))
		done1 := false
		defer stop1()
		next2, stop2 := iter.Pull(Sequence[E2](s2))
		done2 := false
		defer stop2()
		e1 := new(E1)
		e2 := new(E2)
		for {
			if !done1 {
				*e1, done1 = next1()
				if done1 {
					e1 = fill1
				}
			}
			if !done2 {
				*e2, done2 = next2()
				if done2 {
					e2 = fill2
				}
			}
			if done1 && done2 {
				return
			}
			if !yield(e1, e2) {
				return
			}
		}
	}
}

func zipLongest[E1 any, E2 any, S1 Sequenceable[E1], S2 Sequenceable[E2]](s1 S1, s2 S2, fill1 *E1, fill2 *E2) iter.Seq2[*E1, *E2] {
	switch s1 := any(s1).(type) {
	case []E1:
		switch s2 := any(s2).(type) {
		case []E2:
			return zipSliceSlice(s1, s2, fill1, fill2)
		case iter.Seq[E2]:
			return zipSliceSeq(s1, s2, fill1, fill2)
		}
	case iter.Seq[E1]:
		switch s2 := any(s2).(type) {
		case []E2:
			return zipSeqSlice(s1, s2, fill1, fill2)
		case iter.Seq[E2]:
			return zipSeqSeq(s1, s2, fill1, fill2)
		}
	}
	panic("A Sequenceable was neither a slice nor an iter.Seq. This should be impossible")
}

func ZipLongest[E1 any, E2 any, S1 Sequenceable[E1], S2 Sequenceable[E2]](s1 S1, s2 S2, fill1 *E1, fill2 *E2) iter.Seq2[E1, E2] {
	if fill1 == nil {
		fill1 = new(E1)
	}
	if fill2 == nil {
		fill2 = new(E2)
	}
	return Fmap2(zipLongest(s1, s2, fill1, fill2),
		func(e1 *E1, e2 *E2) (E1, E2) {
			return *e1, *e2
		},
	)
}

func Zip[E1 any, E2 any, S1 Sequenceable[E1], S2 Sequenceable[E2]](s1 S1, s2 S2) iter.Seq2[E1, E2] {
	return func(yield func(E1, E2) bool) {
		for e1, e2 := range zipLongest[E1, E2](s1, s2, nil, nil) {
			if e1 == nil || e2 == nil {
				return
			}
			if !yield(*e1, *e2) {
				return
			}
		}
	}
}

func Combine[I1 any, I2 any, O any](s iter.Seq2[I1, I2], f func(I1, I2) O) iter.Seq[O] {
	return func(yield func(O) bool) {
		for e1, e2 := range s {
			if !yield(f(e1, e2)) {
				return
			}
		}
	}
}

func Bifurcate[I any, O1 any, O2 any, S Sequenceable[I]](s S, f func(I) (O1, O2)) iter.Seq2[O1, O2] {
	return func(yield func(O1, O2) bool) {
		for e := range Sequence[I](s) {
			if !yield(f(e)) {
				return
			}
		}
	}
}

func Pair[E1 any, E2 any](s iter.Seq2[E1, E2]) iter.Seq[ElementPair[E1, E2]] {
	return Combine(s, func(e1 E1, e2 E2) ElementPair[E1, E2] {
		return ElementPair[E1, E2]{&e1, &e2}
	})
}

func Unpair[E1 any, E2 any](s iter.Seq[ElementPair[E1, E2]]) iter.Seq2[E1, E2] {
	return Bifurcate(s, func(e ElementPair[E1, E2]) (E1, E2) {
		return *e.First, *e.Second
	})
}

func Swap[E1 any, E2 any, S iter.Seq2[E1, E2]](s S) iter.Seq2[E2, E1] {
	return func(yield func(E2, E1) bool) {
		for e1, e2 := range s {
			if !yield(e2, e1) {
				return
			}
		}
	}
}
