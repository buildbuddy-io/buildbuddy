//go:build darwin && !ios
// +build darwin,!ios

package soci_store

import (
	"github.com/buildbuddy-io/buildbuddy/v2/server/environment"
)

func Init(env environment.Env) (Store, error) {
	return &NoStore{}, nil
}
