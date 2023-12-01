package soci_store

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

// An abstraction of the soci-store that enables podman image streaming.
type Store interface {
	// Returns nil if the soci-store is alive and ready to serve traffic, or an
	// error if not.
	Ready(ctx context.Context) error

	// Waits until the store is alive and ready to serve traffic, returning
	// an error if it takes too lnog.
	WaitUntilReady() error

	// Returns the command-line arguments to pass to podman in order to stream
	// images using this soci-store.
	GetPodmanArgs() []string

	// Fetches the soci artifacts the provided image and stores them locally
	// for this soci-store to use.
	GetArtifacts(ctx context.Context, env environment.Env, image string, creds oci.Credentials) error

	// Passes the provided credentials for the provided image to the store so
	// it can fetch spans of password-protected images.
	PutCredentials(ctx context.Context, image string, credentials oci.Credentials) error
}

// A SociStore implementation that does not start up a soci-store and causes
// regular (slow) image pull behavior when used by podman.
type NoStore struct{}

func (_ NoStore) Ready(ctx context.Context) error {
	return nil
}

func (_ NoStore) WaitUntilReady() error {
	return nil
}

func (_ NoStore) GetPodmanArgs() []string {
	return []string{}
}

func (_ NoStore) GetArtifacts(ctx context.Context, env environment.Env, image string, creds oci.Credentials) error {
	return nil
}

func (_ NoStore) PutCredentials(ctx context.Context, image string, credentials oci.Credentials) error {
	return nil
}
