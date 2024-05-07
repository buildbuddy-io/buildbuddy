package expire

import (
	"math"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type Option int

type Options int

const (
	NONE = Option(iota)
	LT = Option(1 << iota)
	GT = Option(1 << iota)
	NX = Option(1 << iota)
	XX = Option(1 << iota)
)

func NewOptions(opts ... Option) Options {
	opt := Options(NONE)
	for _, o := range opts {
		opt = Options(int(opt) | int(o))
	}
	return opt
}

func HasOptions(opt Options, opts ... Option) bool {
	all := NewOptions(opts...)
	return int(opt) & int(all) == int(all)
}

func ExpandOptions(opts Options) []Option {
	if opts == Options(NONE) || HasOptions(opts, LT, GT) || HasOptions(opts, XX, NX) {
		return []Option{NONE}
	}
	expandedOpts := []Option{}

	if HasOptions(opts, XX) {
		expandedOpts = append(expandedOpts, XX)
	}
	if HasOptions(opts, GT) {
		expandedOpts = append(expandedOpts, GT)
	}
	if HasOptions(opts, LT) {
		expandedOpts = append(expandedOpts, LT)
	} else if HasOptions(opts, NX) {
		// NX is a no-op with LT
		expandedOpts = append(expandedOpts, NX)
	}

	return expandedOpts
}

type Expiry struct {
	Duration time.Duration
	Opt Option
}

// Collapse collapses a list of expiries to its minimal representation in order
// to reduce load on redis when used in combination with the CommandBuffer.
func Collapse(in []*Expiry) ([]*Expiry, error) {
	var x, y, z *time.Duration
	for i, e := range(in) {
		a := e.Duration
		// notset is whether it is possible that no expiry is set
		switch e.Opt {
		case NONE:
			return collapseValue(a, in[i+1:])
		case LT:
			if x != nil && a <= *x {
				if z == nil || a <= *z {
					return collapseValue(a, in[i+1:])
				}
				return collapseValuePair(a, z, in[i+1:])
			} else if y == nil || a <= *y {
				if z == nil || a <= *z {
					return collapseRange(x, a, in[i+1:])
				}
				y = &a
			} else if z == nil || a <= *z {
				z = &a
			}
		case GT:
			if y != nil && a >= *y {
				if z != nil && a >= *z {
					return collapseValue(a, in[i+1:])
				}
				return collapseValuePair(a, z, in[i+1:])
			} else if x == nil || a > *x {
				// can't collapseRange because GT can't guarantee volatility like LT.
				x = &a
			}
			if z != nil && a > *z {
				z = &a
			}
		case NX:
			if z == nil {
				if y != nil && a == *y {
					return collapseRange(x, a, in[i+1:])
				}
				z = &a
			}
		case XX:
			if z == nil {
				return collapseValuePair(a, z, in[i+1:])
			}
			return collapseValue(a, in[i+1:]) 
		default:
			return nil, status.InvalidArgumentErrorf("Unknown redis expire command option: %v", e.Opt)
		}
	}

	expiries := []*Expiry{}
	if x != nil {
		expiries = append(expiries, &Expiry{*x, GT})
	}
	if z != nil {
		expiries = append(expiries, &Expiry{*z, NX})
	}
	if y != nil {
		expiries = append(expiries, &Expiry{*y, LT})
	}

	return expiries, nil
}

// collapseRange only works if LT is the bound for any previously non-volatile expiry
func collapseRange(x *time.Duration, y time.Duration, in []*Expiry) ([]*Expiry, error) {
	for i, e := range(in) {
		a := e.Duration
		switch e.Opt {
		case NONE:
			return collapseValue(a, in[i+1:])
		case LT:
			if x != nil && a <= *x {
				return collapseValue(a, in[i+1:])
			}
			if a < y {
				y = a
			}
		case GT:
			if a >= y {
				return collapseValue(a, in[i+1:])
			}
			if x == nil || a > *x {
				x = &a
			}
		case NX:
		case XX:
			return collapseValue(a, in[i+1:])
		default:
			return nil, status.InvalidArgumentErrorf("Unknown redis expire command option: %v", e.Opt)
		}
	}
	expiries := []*Expiry{}
	if x != nil {
		expiries = append(expiries, &Expiry{*x, GT})
	}
	expiries = append(expiries, &Expiry{y, LT})
	return expiries, nil
}

func collapseValuePair(x time.Duration, y *time.Duration, in []*Expiry) ([]*Expiry, error) {
	for i, e := range(in) {
		a := e.Duration
		switch e.Opt {
		case NONE:
			return collapseValue(a, in[i+1:])
		case LT:
			if a <= x {
				if y == nil || a <= *y {
					return collapseValue(a, in[i+1:])
				}
				x = a
			} else if y == nil || a < *y {
				y = &a
			}
		case GT:
			if a >= x {
				if y != nil && a >= *y {
					return collapseValue(a, in[i+1:])
				}
				x = a
			} else if y != nil && a > *y {
				y = &a
			}
		case NX:
			if y == nil {
				if a == x {
					return collapseValue(a, in[i+1:])
				}
				y = &a
			}
		case XX:
			if y != nil {
				return collapseValue(a, in[i+1:])
			}
			x = a
		default:
			return nil, status.InvalidArgumentErrorf("Unknown redis expire command option: %v", e.Opt)
		}
	}
	expiries := []*Expiry{{x, XX}}
	if y != nil {
		expiries = append(expiries, &Expiry{*y, NX})
	}
	return expiries, nil
}

func collapseValue(x time.Duration, in []*Expiry) ([]*Expiry, error) {
	for _, e := range(in) {
		a := e.Duration
		switch e.Opt {
		case NONE:
			x = a
		case LT:
			if a < x {
				x = a
			}
		case GT:
			if a > x {
				x = a
			}
		case NX:
		case XX:
			x = a
		default:
			return nil, status.InvalidArgumentErrorf("Unknown redis expire command option: %v", e.Opt)
		}
	}
	return []*Expiry{{x, NONE}}, nil
}
