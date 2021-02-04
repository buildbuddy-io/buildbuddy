package capabilities

import (
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

func FromInt(m int32) []akpb.ApiKey_Capability {
	caps := []akpb.ApiKey_Capability{}
	c := int32(1)
	for m > 0 {
		if m&1 > 0 {
			caps = append(caps, akpb.ApiKey_Capability(c))
		}
		m >>= 1
		c <<= 1
	}
	return caps
}

func ToInt(caps []akpb.ApiKey_Capability) int32 {
	m := int32(0)
	for _, c := range caps {
		m |= int32(c)
	}
	return m
}
