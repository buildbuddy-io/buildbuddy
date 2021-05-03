package terminal

var emptyNode = node{blob: ' ', style: &emptyStyle}

type node struct {
	style *style
	elem  *element
	blob  rune
}

func (n *node) hasSameStyle(o node) bool {
	return n.style.isEqual(o.style)
}
