package lookuplist

import "github.com/buildbuddy-io/buildbuddy/server/util/lookuplist/internal"

type LookupNode[K comparable, V any] struct {
	prevKey K
	nextKey K
	value   V
}

type DoublyLinkedListWithKeyLookup[K comparable, V any] struct {
	endpoints internal.Endpoints[K]
	m       map[K]*internal.LookupNode[K, V]
}

func NewDoublyLinkedListWithKeyLookup[K comparable, V any]() *DoublyLinkedListWithKeyLookup[K, V] {
	return &DoublyLinkedListWithKeyLookup[K, V]{
		m: make(map[K]*internal.LookupNode[K, V]),
	}
}

// Empty returns whether or not the list is empty.
func (d *DoublyLinkedListWithKeyLookup[K, V]) Empty() bool {
	return len(d.m) == 0
}

// Lookup looks up a value by key and returns the value, or the zero value of V
// if the key is not found. The returned bool indicates whether or not the key
// was found.
func (d *DoublyLinkedListWithKeyLookup[K, V]) Lookup(key K) (V, bool) {
	v, ok := d.m[key]
	if !ok {
		return d.zeroValue(), ok
	}
	return v.Value, ok
}

// Remove removes the value at the passed key from the list, returning the value
// that was at that key if one existed, or the zero-value of V if not. The
// returned bool indicates whether or not a value existed at that key.
func (d *DoublyLinkedListWithKeyLookup[K, V]) Remove(key K) (V, bool) {
	node := d.removeFromInnerList(key)
	if node == nil {
		return d.zeroValue(), false
	}
	delete(d.m, key)
	return node.Value, true
}

// peek returns the key and value from the front of the list from the given
// orientation, or the zero-values of K and V if the list is empty. The returned
// boolean indicates whether or not the call succeeded.
func (d *DoublyLinkedListWithKeyLookup[K, V]) peek(o internal.Orientation) (K, V, bool) {
	if d.Empty() {
		return d.zeroKey(), d.zeroValue(), false
	}
	return d.endpoints.HeadKey(o), d.m[d.endpoints.HeadKey(o)].Value, true
}

// PeekFront returns the key associated with the head node and the value stored
// in the head node if the list is not empty, and the zero-values of K and V if
// not. The returned bool indicates whether or not the call succeeded.
func (d *DoublyLinkedListWithKeyLookup[K, V]) PeekFront() (K, V, bool) {
	return d.peek(internal.Forward)
}

// PeekBack returns the key associated with the tail node and the value stored
// in the tail node if the list is not empty, and the zero-values of K and V if
// not. The returned bool indicates whether or not the call succeeded.
func (d *DoublyLinkedListWithKeyLookup[K, V]) PeekTail() (K, V, bool) {
	return d.peek(internal.Reverse)
}

// pop removes and returns the key and value from the front of the list from
// the given orientation, or the zero-values of K and V if the list is empty.
// The returned boolen indicates whether or not the call succeeded.
func (d *DoublyLinkedListWithKeyLookup[K, V]) pop(o internal.Orientation) (K, V, bool) {
	key := d.endpoints.HeadKey(o)
	value, ok := d.Remove(key)
	if !ok {
		return d.zeroKey(), d.zeroValue(), false
	}
	return key, value, true
}

// PopFront removes the head node and returns the head key and the head value.
// If the list is empty, it returns the zero-values of K and V. The returned
// bool indicates whether or not the head element was successfully popped.
func (d *DoublyLinkedListWithKeyLookup[K, V]) PopFront() (K, V, bool) {
	return d.pop(internal.Forward)
}

// PopBack removes the tail node and returns the tail key and the tail value.
// If the list is empty, it returns the zero-values of K and V. The returned
// bool indicates whether or not the tail element was successfully popped.
func (d *DoublyLinkedListWithKeyLookup[K, V]) PopBack() (K, V, bool) {
	return d.pop(internal.Reverse)
}

// push pushes the given value onto the end of the list from the given
// orientation and sets its kye to the passed key, removing any value
// previously associated with that key from the list if one exists.
func (d *DoublyLinkedListWithKeyLookup[K, V]) push(key K, value V, o internal.Orientation) {
	if d.Empty() {
		d.endpoints.SetHeadKey(key, o)
		d.endpoints.SetTailKey(key, o)
		d.m[key] = &internal.LookupNode[K, V]{Value: value}
		return
	}
	if key == d.endpoints.TailKey(o) {
		node := d.m[key]
		node.Value = value
		return
	}
	node := d.removeFromInnerList(key)
	if node == nil {
		node = &internal.LookupNode[K, V]{}
		d.m[key] = node
	}
	node.Value = value
	node.SetPrevKey(d.endpoints.TailKey(o), o)
	d.endpoints.SetTailKey(key, o)
}


// PushBack appends the passed value to the end of the list and sets its key to
// the passed key, removing any value previously associated with that key from
// the list if one exists.
func (d *DoublyLinkedListWithKeyLookup[K, V]) PushBack(key K, value V) {
	d.push(key, value, internal.Forward)
}

// PushFront prepends the passed value to the start of the list and sets its key to
// the passed key, removing any value previously associated with that key from
// the list if one exists.
func (d *DoublyLinkedListWithKeyLookup[K, V]) PushFront(key K, value V) {
	d.push(key, value, internal.Reverse)
}

// removeFromInnerList removes the node from the inner list only, not from the
// inner map, returning the node if it existed or nil if it did not. This is an
// symmetric operation, so no orientation needs to be specified.
func (d *DoublyLinkedListWithKeyLookup[K, V]) removeFromInnerList(key K) *internal.LookupNode[K, V] {
	node, ok := d.m[key]
	if !ok {
		return nil
	}
	if len(d.m) == 1 {
		// This was the only node; headKey and tailKey are meaningless for an empty
		// list, and there are no nodes to modify. Nothing to do.
		return node
	}
	for _, o := range([]internal.Orientation{internal.Forward, internal.Reverse}) {
		if key != d.endpoints.TailKey(o) {
			// all nodes except the tail are guaranteed to have a valid prevKey
			nextNode := d.m[node.NextKey(o)]
			nextNode.SetPrevKey(node.PrevKey(o), o)
			if key == d.endpoints.HeadKey(o) {
				d.endpoints.SetHeadKey(node.NextKey(o), o)
			}
		}
	}
	return node
}

// zeroValue returns the zero-value of V. This is abstracted for readability.
func (d *DoublyLinkedListWithKeyLookup[K, V]) zeroValue() V {
	var zero V
	return zero
}

// zeroKey returns the zero-value of K. This is abstracted for readability.
func (d *DoublyLinkedListWithKeyLookup[K, V]) zeroKey() K {
	var zero K
	return zero
}
