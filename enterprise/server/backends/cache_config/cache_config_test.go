package cache_config_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/cache_config"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

// Mirrors the cache.migration flag registered by the migration cache.
var _ = flag.Struct("test.cache_config.migration", cache_config.MigrationConfig{}, "")

func TestMigrationConfigRedactsGCSCredentials(t *testing.T) {
	flags.Set(t, "test.cache_config.migration", cache_config.MigrationConfig{
		Src: &cache_config.CacheConfig{
			PebbleConfig: &cache_config.PebbleCacheConfig{
				RootDirectory: "/tmp/src",
				GCSConfig: cache_config.GCSConfig{
					Bucket:      "src-bucket",
					Credentials: "SRC_GCS_PRIVATE_KEY",
				},
			},
		},
		Dest: &cache_config.CacheConfig{
			MetaConfig: &cache_config.MetaCacheConfig{
				MetadataBackend: "grpc://localhost:1991",
				GCSConfig: cache_config.GCSConfig{
					Bucket:      "dest-bucket",
					Credentials: "DEST_GCS_PRIVATE_KEY",
				},
			},
		},
	})

	flg := flag.CommandLine.Lookup("test.cache_config.migration")
	require.NotNil(t, flg)

	// Render the flag the same way the /flagz page does. (We can't render the
	// whole flagset here since the Go testing package registers flag types
	// that the YAML generator doesn't support.)
	m, err := flagyaml.GenerateDocumentedMarshalerFromFlag(flg)
	require.NoError(t, err)
	n, err := flagyaml.DocumentedNode(m, flagyaml.RedactSecrets)
	require.NoError(t, err)
	b, err := yaml.Marshal(n)
	require.NoError(t, err)
	out := string(b)
	assert.NotContains(t, out, "SRC_GCS_PRIVATE_KEY")
	assert.NotContains(t, out, "DEST_GCS_PRIVATE_KEY")
	// Non-secret fields are still rendered.
	assert.Contains(t, out, "src-bucket")
	assert.Contains(t, out, "dest-bucket")

	// The flag as a whole is treated as secret when logging configured flags.
	assert.True(t, redact.IsSecret(flg))
}
