package internal

type Orientation bool

var (
	Forward = Orientation(true)
	Reverse = Orientation(false)
)

type LookupNode[K comparable, V any] struct {
	prevKey K
	nextKey K
	Value   V
}

func (n *LookupNode[K, V]) NextKey(o Orientation) K {
	if o {
		return n.nextKey
	} else {
		return n.prevKey
	}
}

func (n *LookupNode[K, V]) PrevKey(o Orientation) K {
	if o {
		return n.prevKey
	} else {
		return n.nextKey
	}
}

func (n *LookupNode[K, V]) SetNextKey(key K, o Orientation) {
	if o {
		n.nextKey = key
	} else {
		n.prevKey = key
	}
}

func (n *LookupNode[K, V]) SetPrevKey(key K, o Orientation) {
	if o {
		n.prevKey = key
	} else {
		n.nextKey = key
	}
}

type Endpoints[K comparable] struct {
	headKey K
	tailKey K
}

func (e *Endpoints[K]) HeadKey(o Orientation) K {
	if o {
		return e.headKey
	} else {
		return e.tailKey
	}
}

func (e *Endpoints[K]) TailKey(o Orientation) K {
	if o {
		return e.tailKey
	} else {
		return e.headKey
	}
}

func (e *Endpoints[K]) SetHeadKey(key K, o Orientation) {
	if o {
		e.headKey = key
	} else {
		e.tailKey = key
	}
}

func (e *Endpoints[K]) SetTailKey(key K, o Orientation) {
	if o {
		e.tailKey = key
	} else {
		e.headKey = key
	}
}

