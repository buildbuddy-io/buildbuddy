package soci_store

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

// TODO: refactor a bit more
// TODO: write some more comments here and there
// TODO: add tests
// TODO: perform tests

type Store interface {
	Exists(ctx context.Context) error
	WaitUntilExists() error

	// Returns the command-line argument to pass to podman in order to stream
	// images using this soci-store.
	EnableStreamingStoreArg() string

	GetArtifacts(ctx context.Context, env environment.Env, image string, creds oci.Credentials) error

	SeedCredentials(ctx context.Context, image string, credentials oci.Credentials) error
}

// A SociStore implementation that does not start up a soci-store and does not
// stream container images.
type NoStore struct{}

func (_ NoStore) Exists(ctx context.Context) error {
	return nil
}

func (_ NoStore) WaitUntilExists() error {
	return nil
}

func (_ NoStore) EnableStreamingStoreArg() string {
	return ""
}

func (_ NoStore) GetArtifacts(ctx context.Context, env environment.Env, image string, creds oci.Credentials) error {
	return nil
}

func (_ NoStore) SeedCredentials(ctx context.Context, image string, credentials oci.Credentials) error {
	return nil
}
