// Package ocifetchertest provides shared access-control tests that exercise
// the three claims the OCIFetcher implementations are required to uphold:
//
//  1. Anonymous requests do not write to or read from the AC or CAS.
//  2. If bypass_registry is set, the request must come from a server admin;
//     otherwise the response is an error and the registry is not contacted.
//  3. Manifests, blobs, and metadata fetched with credentials cannot be
//     served from the AC or CAS without credentials.
//
// The tests are written against the OCIFetcherClient gRPC surface so that
// both ocifetcher (the in-app service) and ocifetcher_server_proxy (the
// executor-side proxy) can share them.
//
// Tests that exercise claims not yet implemented in the production code
// path call t.Skip with a reference to the open PR that adds enforcement.
// Once enforcement lands, removing the t.Skip line is the only edit needed
// to enable the test.
package ocifetchertest

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

// AdminGroupID is the group id used for the admin user in the shared suite.
// The suite configures the `auth.admin_group_id` flag to this value at the
// top of each test that needs it.
const AdminGroupID = "GR-ocifetchertest-admin"

// AdminUser is the authenticated user the suite uses for server-admin
// gated paths (i.e., bypass_registry).
var AdminUser = &claims.Claims{
	UserID:        "US-admin",
	GroupID:       AdminGroupID,
	AllowedGroups: []string{AdminGroupID},
	GroupMemberships: []*interfaces.GroupMembership{
		{
			GroupID:      AdminGroupID,
			Capabilities: []cappb.Capability{cappb.Capability_ORG_ADMIN},
		},
	},
}

// NonAdminUser is an authenticated, non-admin user the suite uses to
// exercise the "non-admin must be denied" half of claim 2.
var NonAdminUser = &claims.Claims{
	UserID:        "US-non-admin",
	GroupID:       "GR-non-admin",
	AllowedGroups: []string{"GR-non-admin"},
}

// AuthedUser is a plain authenticated user the suite uses to populate the
// cache before exercising anonymous / wrong-credentials reads.
var AuthedUser = &claims.Claims{
	UserID:  "US-authed",
	GroupID: "GR-authed",
}

// PrivateRegistryCreds are the credentials the suite configures on the
// private test registry. Tests use these as the "valid" credentials when
// pre-populating the cache.
var PrivateRegistryCreds = &testregistry.BasicAuthCreds{
	Username: "ocifetchertest-user",
	Password: "ocifetchertest-pass",
}

// SetupFunc creates an OCIFetcherClient that talks to the implementation
// under test, configured to fetch from the given registry.
//
// Setup functions are expected to:
//   - wire the implementation's environment with a JWT-parsing
//     authenticator so admin claims set via testauth.WithAuthenticatedUserInfo
//     are honored end-to-end;
//   - set executor.container_registry_allowed_private_ips so the test
//     registry (running on localhost) is reachable.
type SetupFunc func(t *testing.T, ctx context.Context, reg *testregistry.Registry) ofpb.OCIFetcherClient

// RunAccessControlTests runs the shared access-control test suite against
// the given implementation factory.
func RunAccessControlTests(t *testing.T, setup SetupFunc) {
	t.Run("AnonymousSkipsCache", func(t *testing.T) {
		runAnonymousSkipsCache(t, setup)
	})
	t.Run("BypassRegistryRequiresAdmin", func(t *testing.T) {
		runBypassRegistryRequiresAdmin(t, setup)
	})
	t.Run("CredentialedCacheRequiresCreds", func(t *testing.T) {
		runCredentialedCacheRequiresCreds(t, setup)
	})
}

// -----------------------------------------------------------------------------
// Anonymous requests do not read from or write to AC/CAS.
// -----------------------------------------------------------------------------
//
// Strategy: pre-populate the cache via an authenticated fetch, then make two
// back-to-back anonymous fetches. The anonymous fetches must each contact the
// upstream registry — proving both (a) the anonymous request did not read the
// pre-populated cache entry, and (b) the first anonymous request did not
// write a new cache entry that the second could read.
//
// Anonymous == no authenticated user info on the context. The shared suite
// uses context.Background() to represent that.

func runAnonymousSkipsCache(t *testing.T, setup SetupFunc) {
	t.Run("FetchManifest", func(t *testing.T) {
		t.Skip("anonymous skip cache not yet enforced for FetchManifest; tracking PR #12175")
		ctx := context.Background()
		reg, counter := newAnonymousRegistry(t)
		imageName, img := reg.PushNamedImage(t, "anon-fetchmanifest", nil)
		manifestDigest, _, _ := imageMetadata(t, img)
		ref := imageName + "@" + manifestDigest
		client := setup(t, ctx, reg)

		// Pre-populate the manifest AC via an authenticated fetch.
		_, err := client.FetchManifest(authedCtx(ctx), &ofpb.FetchManifestRequest{Ref: ref})
		require.NoError(t, err)

		manifestPath := http.MethodGet + " /v2/" + repoOf(imageName) + "/manifests/" + manifestDigest
		counter.Reset()
		_, err = client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: ref})
		require.NoError(t, err)
		require.Positive(t, counter.Snapshot()[manifestPath],
			"anonymous FetchManifest should bypass AC and contact the registry")

		counter.Reset()
		_, err = client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: ref})
		require.NoError(t, err)
		require.Positive(t, counter.Snapshot()[manifestPath],
			"second anonymous FetchManifest should also bypass AC (proves first call did not populate it)")
	})

	t.Run("FetchManifestMetadata", func(t *testing.T) {
		// FetchManifestMetadata never reads or writes cache by design,
		// so anonymous requests bypass it today. Keep the test live as
		// a regression guard.
		ctx := context.Background()
		reg, counter := newAnonymousRegistry(t)
		imageName, img := reg.PushNamedImage(t, "anon-fetchmanifestmeta", nil)
		manifestDigest, _, _ := imageMetadata(t, img)
		ref := imageName + "@" + manifestDigest
		client := setup(t, ctx, reg)

		_, err := client.FetchManifestMetadata(authedCtx(ctx), &ofpb.FetchManifestMetadataRequest{Ref: ref})
		require.NoError(t, err)

		manifestHead := http.MethodHead + " /v2/" + repoOf(imageName) + "/manifests/" + manifestDigest
		for i := 0; i < 2; i++ {
			counter.Reset()
			_, err = client.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{Ref: ref})
			require.NoError(t, err)
			require.Positive(t, counter.Snapshot()[manifestHead],
				"anonymous FetchManifestMetadata #%d must contact the registry (no caching)", i+1)
		}
	})

	t.Run("FetchBlobMetadata", func(t *testing.T) {
		t.Skip("anonymous skip cache not yet enforced for FetchBlobMetadata; tracking PR #12175")
		ctx := context.Background()
		reg, counter := newAnonymousRegistry(t)
		imageName, img := reg.PushNamedImage(t, "anon-fetchblobmeta", nil)
		layerDigest := firstLayerDigest(t, img)
		ref := imageName + "@" + layerDigest
		client := setup(t, ctx, reg)

		// Pre-populate blob metadata in AC via authenticated FetchBlob
		// (which is what writes blob metadata, not FetchBlobMetadata).
		require.NoError(t, doFetchBlob(client, authedCtx(ctx), &ofpb.FetchBlobRequest{Ref: ref}))

		blobHead := http.MethodHead + " /v2/" + repoOf(imageName) + "/blobs/" + layerDigest
		for i := 0; i < 2; i++ {
			counter.Reset()
			_, err := client.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{Ref: ref})
			require.NoError(t, err)
			require.Positive(t, counter.Snapshot()[blobHead],
				"anonymous FetchBlobMetadata #%d must bypass AC and contact the registry", i+1)
		}
	})

	t.Run("FetchBlob", func(t *testing.T) {
		t.Skip("anonymous skip cache not yet enforced for FetchBlob; tracking PR #12175")
		ctx := context.Background()
		reg, counter := newAnonymousRegistry(t)
		imageName, img := reg.PushNamedImage(t, "anon-fetchblob", nil)
		layerDigest := firstLayerDigest(t, img)
		ref := imageName + "@" + layerDigest
		client := setup(t, ctx, reg)

		// Pre-populate the blob in BS and its metadata in AC via an
		// authenticated FetchBlob.
		require.NoError(t, doFetchBlob(client, authedCtx(ctx), &ofpb.FetchBlobRequest{Ref: ref}))

		blobGet := http.MethodGet + " /v2/" + repoOf(imageName) + "/blobs/" + layerDigest
		for i := 0; i < 2; i++ {
			counter.Reset()
			require.NoError(t, doFetchBlob(client, ctx, &ofpb.FetchBlobRequest{Ref: ref}))
			require.Positive(t, counter.Snapshot()[blobGet],
				"anonymous FetchBlob #%d must bypass BS/AC and contact the registry", i+1)
		}
	})
}

// -----------------------------------------------------------------------------
// bypass_registry requires a server admin.
// -----------------------------------------------------------------------------
//
// Strategy: pre-populate the cache via an authenticated fetch. Then for each
// RPC, exercise the cartesian product of {admin, non-admin, anonymous} x
// {bypass_registry=true}. Non-admin and anonymous must be denied without
// touching the upstream registry; admin must serve the cached entry, also
// without touching the upstream registry.

func runBypassRegistryRequiresAdmin(t *testing.T, setup SetupFunc) {
	flags.Set(t, "auth.admin_group_id", AdminGroupID)

	t.Run("FetchManifest", func(t *testing.T) {
		ctx := context.Background()
		reg, counter := newAnonymousRegistry(t)
		imageName, img := reg.PushNamedImage(t, "bypass-fetchmanifest", nil)
		manifestDigest, _, _ := imageMetadata(t, img)
		ref := imageName + "@" + manifestDigest
		client := setup(t, ctx, reg)

		// Pre-populate manifest AC entry.
		_, err := client.FetchManifest(authedCtx(ctx), &ofpb.FetchManifestRequest{Ref: ref})
		require.NoError(t, err)

		t.Run("AnonymousDenied", func(t *testing.T) {
			counter.Reset()
			_, err := client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: ref, BypassRegistry: true})
			require.Error(t, err)
			require.True(t, isAuthError(err), "anonymous bypass must be denied, got: %v", err)
			require.Empty(t, counter.Snapshot(), "denied bypass must not contact the registry")
		})
		t.Run("NonAdminDenied", func(t *testing.T) {
			counter.Reset()
			_, err := client.FetchManifest(userCtx(NonAdminUser), &ofpb.FetchManifestRequest{Ref: ref, BypassRegistry: true})
			require.Error(t, err)
			require.True(t, status.IsPermissionDeniedError(err), "non-admin bypass must be denied with PermissionDenied, got: %v", err)
			require.Empty(t, counter.Snapshot(), "denied bypass must not contact the registry")
		})
		t.Run("AdminServedFromCache", func(t *testing.T) {
			counter.Reset()
			resp, err := client.FetchManifest(userCtx(AdminUser), &ofpb.FetchManifestRequest{Ref: ref, BypassRegistry: true})
			require.NoError(t, err)
			require.Equal(t, manifestDigest, resp.GetDigest())
			require.Empty(t, counter.Snapshot(), "admin bypass with cache hit must not contact the registry")
		})
	})

	t.Run("FetchManifestMetadata", func(t *testing.T) {
		ctx := context.Background()
		reg, counter := newAnonymousRegistry(t)
		imageName, img := reg.PushNamedImage(t, "bypass-fetchmanifestmeta", nil)
		manifestDigest, _, _ := imageMetadata(t, img)
		ref := imageName + "@" + manifestDigest
		client := setup(t, ctx, reg)

		// FetchManifestMetadata doesn't write to AC on its own, but a
		// preceding FetchManifest does — the resulting cache entry is
		// what FetchManifestMetadata/bypass would read from once the
		// admin-readable cache lookup lands (PR #12175 introduces it).
		_, err := client.FetchManifest(authedCtx(ctx), &ofpb.FetchManifestRequest{Ref: ref})
		require.NoError(t, err)

		t.Run("AnonymousDenied", func(t *testing.T) {
			counter.Reset()
			_, err := client.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{Ref: ref, BypassRegistry: true})
			require.Error(t, err)
			require.True(t, isAuthError(err), "anonymous bypass must be denied, got: %v", err)
			require.Empty(t, counter.Snapshot(), "denied bypass must not contact the registry")
		})
		t.Run("NonAdminDenied", func(t *testing.T) {
			counter.Reset()
			_, err := client.FetchManifestMetadata(userCtx(NonAdminUser), &ofpb.FetchManifestMetadataRequest{Ref: ref, BypassRegistry: true})
			require.Error(t, err)
			require.True(t, status.IsPermissionDeniedError(err), "non-admin bypass must be denied with PermissionDenied, got: %v", err)
			require.Empty(t, counter.Snapshot(), "denied bypass must not contact the registry")
		})
		t.Run("AdminBypassNotSupported", func(t *testing.T) {
			counter.Reset()
			_, err := client.FetchManifestMetadata(userCtx(AdminUser), &ofpb.FetchManifestMetadataRequest{Ref: ref, BypassRegistry: true})
			// Current implementation returns NotFound for admin bypass on
			// FetchManifestMetadata because it is not yet supported.
			// When support lands, replace this assertion with a successful
			// cache-read assertion instead of accepting both outcomes.
			require.Error(t, err)
			require.True(t, status.IsNotFoundError(err), "admin bypass is not supported yet; got: %v", err)
			require.Empty(t, counter.Snapshot(), "admin bypass must never contact the registry")
		})
	})

	t.Run("FetchBlobMetadata", func(t *testing.T) {
		ctx := context.Background()
		reg, counter := newAnonymousRegistry(t)
		imageName, img := reg.PushNamedImage(t, "bypass-fetchblobmeta", nil)
		layerDigest := firstLayerDigest(t, img)
		ref := imageName + "@" + layerDigest
		client := setup(t, ctx, reg)

		// FetchBlob writes blob metadata to AC.
		require.NoError(t, doFetchBlob(client, authedCtx(ctx), &ofpb.FetchBlobRequest{Ref: ref}))

		t.Run("AnonymousDenied", func(t *testing.T) {
			counter.Reset()
			_, err := client.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{Ref: ref, BypassRegistry: true})
			require.Error(t, err)
			require.True(t, isAuthError(err), "anonymous bypass must be denied, got: %v", err)
			require.Empty(t, counter.Snapshot(), "denied bypass must not contact the registry")
		})
		t.Run("NonAdminDenied", func(t *testing.T) {
			counter.Reset()
			_, err := client.FetchBlobMetadata(userCtx(NonAdminUser), &ofpb.FetchBlobMetadataRequest{Ref: ref, BypassRegistry: true})
			require.Error(t, err)
			require.True(t, status.IsPermissionDeniedError(err), "non-admin bypass must be denied with PermissionDenied, got: %v", err)
			require.Empty(t, counter.Snapshot(), "denied bypass must not contact the registry")
		})
		t.Run("AdminServedFromCache", func(t *testing.T) {
			counter.Reset()
			_, err := client.FetchBlobMetadata(userCtx(AdminUser), &ofpb.FetchBlobMetadataRequest{Ref: ref, BypassRegistry: true})
			require.NoError(t, err)
			require.Empty(t, counter.Snapshot(), "admin bypass with cache hit must not contact the registry")
		})
	})

	t.Run("FetchBlob", func(t *testing.T) {
		ctx := context.Background()
		reg, counter := newAnonymousRegistry(t)
		imageName, img := reg.PushNamedImage(t, "bypass-fetchblob", nil)
		layerDigest := firstLayerDigest(t, img)
		ref := imageName + "@" + layerDigest
		client := setup(t, ctx, reg)

		// Pre-populate the blob in BS / metadata in AC.
		require.NoError(t, doFetchBlob(client, authedCtx(ctx), &ofpb.FetchBlobRequest{Ref: ref}))

		t.Run("AnonymousDenied", func(t *testing.T) {
			counter.Reset()
			err := doFetchBlob(client, ctx, &ofpb.FetchBlobRequest{Ref: ref, BypassRegistry: true})
			require.Error(t, err)
			require.True(t, isAuthError(err), "anonymous bypass must be denied, got: %v", err)
			require.Empty(t, counter.Snapshot(), "denied bypass must not contact the registry")
		})
		t.Run("NonAdminDenied", func(t *testing.T) {
			counter.Reset()
			err := doFetchBlob(client, userCtx(NonAdminUser), &ofpb.FetchBlobRequest{Ref: ref, BypassRegistry: true})
			require.Error(t, err)
			require.True(t, status.IsPermissionDeniedError(err), "non-admin bypass must be denied with PermissionDenied, got: %v", err)
			require.Empty(t, counter.Snapshot(), "denied bypass must not contact the registry")
		})
		t.Run("AdminServedFromCache", func(t *testing.T) {
			counter.Reset()
			err := doFetchBlob(client, userCtx(AdminUser), &ofpb.FetchBlobRequest{Ref: ref, BypassRegistry: true})
			require.NoError(t, err)
			require.Empty(t, counter.Snapshot(), "admin bypass with cache hit must not contact the registry")
		})
	})
}

// -----------------------------------------------------------------------------
// Cache entries fetched with credentials cannot be served to unauthenticated
// or wrong-credential requests.
// -----------------------------------------------------------------------------
//
// Strategy: configure the test registry with basic-auth credentials, then
// pre-populate the cache via authenticated calls that present valid
// credentials. Subsequent calls with missing/invalid credentials must fail
// with Unauthenticated and must not serve the cached data.

func runCredentialedCacheRequiresCreds(t *testing.T, setup SetupFunc) {
	type credCase struct {
		name string
		// nil means "no credentials".
		creds *rgpb.Credentials
	}
	credCases := []credCase{
		{"MissingCredentials", nil},
		{"InvalidCredentials", &rgpb.Credentials{Username: "wrong", Password: "wrong"}},
	}
	validCreds := &rgpb.Credentials{
		Username: PrivateRegistryCreds.Username,
		Password: PrivateRegistryCreds.Password,
	}

	t.Run("FetchManifest", func(t *testing.T) {
		ctx := context.Background()
		reg, _ := newPrivateRegistry(t)
		imageName, img := reg.PushNamedImage(t, "creds-fetchmanifest", PrivateRegistryCreds)
		manifestDigest, _, _ := imageMetadata(t, img)
		ref := imageName + "@" + manifestDigest
		client := setup(t, ctx, reg)

		// Populate the AC entry using valid credentials.
		_, err := client.FetchManifest(authedCtx(ctx), &ofpb.FetchManifestRequest{Ref: ref, Credentials: validCreds})
		require.NoError(t, err)

		for _, cc := range credCases {
			t.Run(cc.name, func(t *testing.T) {
				t.Skip("creds required for cache reads not yet enforced for FetchManifest; tracking PR #12196")
				_, err := client.FetchManifest(authedCtx(ctx), &ofpb.FetchManifestRequest{Ref: ref, Credentials: cc.creds})
				require.Error(t, err)
				require.True(t, status.IsUnauthenticatedError(err),
					"FetchManifest with %s must return Unauthenticated, got: %v", cc.name, err)
			})
		}
	})

	t.Run("FetchManifestMetadata", func(t *testing.T) {
		// FetchManifestMetadata always contacts the registry, so wrong
		// credentials surface as 401 from upstream — this is already
		// enforced today.
		ctx := context.Background()
		reg, _ := newPrivateRegistry(t)
		imageName, img := reg.PushNamedImage(t, "creds-fetchmanifestmeta", PrivateRegistryCreds)
		manifestDigest, _, _ := imageMetadata(t, img)
		ref := imageName + "@" + manifestDigest
		client := setup(t, ctx, reg)

		_, err := client.FetchManifest(authedCtx(ctx), &ofpb.FetchManifestRequest{Ref: ref, Credentials: validCreds})
		require.NoError(t, err)

		for _, cc := range credCases {
			t.Run(cc.name, func(t *testing.T) {
				_, err := client.FetchManifestMetadata(authedCtx(ctx), &ofpb.FetchManifestMetadataRequest{Ref: ref, Credentials: cc.creds})
				require.Error(t, err)
				require.True(t, status.IsUnauthenticatedError(err),
					"FetchManifestMetadata with %s must return Unauthenticated, got: %v", cc.name, err)
			})
		}
	})

	t.Run("FetchBlobMetadata", func(t *testing.T) {
		ctx := context.Background()
		reg, _ := newPrivateRegistry(t)
		imageName, img := reg.PushNamedImage(t, "creds-fetchblobmeta", PrivateRegistryCreds)
		layerDigest := firstLayerDigest(t, img)
		ref := imageName + "@" + layerDigest
		client := setup(t, ctx, reg)

		require.NoError(t, doFetchBlob(client, authedCtx(ctx), &ofpb.FetchBlobRequest{Ref: ref, Credentials: validCreds}))

		for _, cc := range credCases {
			t.Run(cc.name, func(t *testing.T) {
				t.Skip("creds required for cache reads not yet enforced for FetchBlobMetadata; tracking PR #12196")
				_, err := client.FetchBlobMetadata(authedCtx(ctx), &ofpb.FetchBlobMetadataRequest{Ref: ref, Credentials: cc.creds})
				require.Error(t, err)
				require.True(t, status.IsUnauthenticatedError(err),
					"FetchBlobMetadata with %s must return Unauthenticated, got: %v", cc.name, err)
			})
		}
	})

	t.Run("FetchBlob", func(t *testing.T) {
		ctx := context.Background()
		reg, _ := newPrivateRegistry(t)
		imageName, img := reg.PushNamedImage(t, "creds-fetchblob", PrivateRegistryCreds)
		layerDigest := firstLayerDigest(t, img)
		ref := imageName + "@" + layerDigest
		client := setup(t, ctx, reg)

		require.NoError(t, doFetchBlob(client, authedCtx(ctx), &ofpb.FetchBlobRequest{Ref: ref, Credentials: validCreds}))

		for _, cc := range credCases {
			t.Run(cc.name, func(t *testing.T) {
				t.Skip("creds required for cache reads not yet enforced for FetchBlob; tracking PR #12196")
				err := doFetchBlob(client, authedCtx(ctx), &ofpb.FetchBlobRequest{Ref: ref, Credentials: cc.creds})
				require.Error(t, err)
				require.True(t, status.IsUnauthenticatedError(err),
					"FetchBlob with %s must return Unauthenticated, got: %v", cc.name, err)
			})
		}
	})
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// newAnonymousRegistry runs a test registry with no auth and attaches a
// request counter that observes every incoming HTTP request.
func newAnonymousRegistry(t *testing.T) (*testregistry.Registry, *testhttp.RequestCounter) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(_ http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	return reg, counter
}

// newPrivateRegistry runs a test registry that requires PrivateRegistryCreds
// for access and attaches a request counter.
func newPrivateRegistry(t *testing.T) (*testregistry.Registry, *testhttp.RequestCounter) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		Creds: PrivateRegistryCreds,
		HttpInterceptor: func(_ http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	return reg, counter
}

func imageMetadata(t *testing.T, img v1.Image) (digest string, size int64, mediaType string) {
	d, err := img.Digest()
	require.NoError(t, err)
	s, err := img.Size()
	require.NoError(t, err)
	m, err := img.MediaType()
	require.NoError(t, err)
	return d.String(), s, string(m)
}

func firstLayerDigest(t *testing.T, img v1.Image) string {
	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	d, err := layers[0].Digest()
	require.NoError(t, err)
	return d.String()
}

// repoOf extracts the repository path from a "<registry>/<repo>" image
// reference. Test registries return refs of the form
// "localhost:<port>/<repo>"; the registry URL path is /v2/<repo>/...
func repoOf(imageName string) string {
	// imageName is "host:port/<repo>"; strip everything up to and including
	// the first "/".
	for i := 0; i < len(imageName); i++ {
		if imageName[i] == '/' {
			return imageName[i+1:]
		}
	}
	return imageName
}

// authedCtx returns a context with claims for a plain authenticated user.
// The proxy implementation requires *some* user to be present in order to
// touch the cache today (claim 1 is not yet implemented at the proxy
// either; PR #12175 adds it). This helper keeps the suite working against
// both implementations.
func authedCtx(ctx context.Context) context.Context {
	return testauth.WithAuthenticatedUserInfo(ctx, AuthedUser)
}

// userCtx returns a context with claims for the given user.
func userCtx(u *claims.Claims) context.Context {
	return testauth.WithAuthenticatedUserInfo(context.Background(), u)
}

// isAuthError returns true if the error is either Unauthenticated or
// PermissionDenied.
func isAuthError(err error) bool {
	return status.IsUnauthenticatedError(err) || status.IsPermissionDeniedError(err)
}

// doFetchBlob runs a FetchBlob streaming RPC and drains the response.
// It returns the first error encountered (either from opening the stream or
// during streaming).
func doFetchBlob(client ofpb.OCIFetcherClient, ctx context.Context, req *ofpb.FetchBlobRequest) error {
	stream, err := client.FetchBlob(ctx, req)
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
