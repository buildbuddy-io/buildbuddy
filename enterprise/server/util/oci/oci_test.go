package oci_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

func TestCredentialsFromProto(t *testing.T) {
	creds, err := oci.CredentialsFromProto(&rgpb.Credentials{
		Username: "",
		Password: "",
	})
	require.NoError(t, err)
	assert.True(t, creds.IsEmpty())

	_, err = oci.CredentialsFromProto(&rgpb.Credentials{
		Username: "foo",
		Password: "",
	})
	require.Error(t, err)

	_, err = oci.CredentialsFromProto(&rgpb.Credentials{
		Username: "",
		Password: "bar",
	})
	require.Error(t, err)

	creds, err = oci.CredentialsFromProto(&rgpb.Credentials{
		Username: "foo",
		Password: "bar",
	})
	require.NoError(t, err)
	assert.False(t, creds.IsEmpty())
	assert.Equal(t, "foo:bar", creds.String())
}

func TestCredentialsFromProperties(t *testing.T) {
	registries := []oci.Registry{
		{
			Hostnames: []string{"gcr.io", "us.gcr.io", "eu.gcr.io", "asia.gcr.io", "marketplace.gcr.io"},
			Username:  "gcruser",
			Password:  "gcrpass",
		},
		{
			Hostnames: []string{"docker.io"},
			Username:  "dockeruser",
			Password:  "dockerpass",
		},
	}
	flags.Set(t, "executor.container_registries", registries)

	props := &platform.Properties{}
	c, err := oci.CredentialsFromProperties(props)
	require.NoError(t, err)
	assert.True(t, c.IsEmpty())

	props = &platform.Properties{
		ContainerImage:            "missing-password.io",
		ContainerRegistryUsername: "username",
		ContainerRegistryPassword: "",
	}
	_, err = oci.CredentialsFromProperties(props)
	assert.True(t, status.IsInvalidArgumentError(err))

	props = &platform.Properties{
		ContainerImage:            "missing-username.io",
		ContainerRegistryUsername: "",
		ContainerRegistryPassword: "password",
	}
	_, err = oci.CredentialsFromProperties(props)
	assert.True(t, status.IsInvalidArgumentError(err))

	for _, testCase := range []struct {
		imageRef            string
		expectedCredentials oci.Credentials
	}{
		// Creds shouldn't be returned if there's no container image requested
		{"", oci.Credentials{}},
		// Creds shouldn't be returned if the registry is unrecognized
		{"unrecognized-registry.io/foo/bar", oci.Credentials{}},
		{"unrecognized-registry.io/foo/bar:latest", oci.Credentials{}},
		{"unrecognized-registry.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", oci.Credentials{}},
		// Images with no domain should get defaulted to docker.io (Docker and
		// other tools like `skopeo` assume docker.io as the default registry)
		{"alpine", creds("dockeruser", "dockerpass")},
		{"alpine:latest", creds("dockeruser", "dockerpass")},
		{"alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		// docker.io supports both `/image` and `/library/image` -- make sure we
		// handle both
		{"docker.io/alpine", creds("dockeruser", "dockerpass")},
		{"docker.io/alpine:latest", creds("dockeruser", "dockerpass")},
		{"docker.io/alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		{"docker.io/library/alpine:latest", creds("dockeruser", "dockerpass")},
		{"docker.io/library/alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		// Non-docker registries should work as well
		{"gcr.io/foo/bar", creds("gcruser", "gcrpass")},
		{"gcr.io/foo/bar:latest", creds("gcruser", "gcrpass")},
		{"gcr.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("gcruser", "gcrpass")},
		// Subdomains should work too
		{"marketplace.gcr.io/foo/bar", creds("gcruser", "gcrpass")},
		{"marketplace.gcr.io/foo/bar:latest", creds("gcruser", "gcrpass")},
		{"marketplace.gcr.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("gcruser", "gcrpass")},
	} {
		props = &platform.Properties{ContainerImage: testCase.imageRef}

		creds, err := oci.CredentialsFromProperties(props)

		assert.NoError(t, err)
		assert.Equal(
			t, testCase.expectedCredentials, creds,
			"unexpected credentials for image ref %q: %q",
			testCase.imageRef, testCase.expectedCredentials,
		)
	}
}

func creds(username, password string) oci.Credentials {
	return oci.Credentials{Username: username, Password: password}
}

func TestToProto(t *testing.T) {
	assert.True(t,
		proto.Equal(
			&rgpb.Credentials{Username: "foo", Password: "bar"},
			oci.Credentials{Username: "foo", Password: "bar"}.ToProto()))
}
