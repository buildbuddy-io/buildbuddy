package webdriver_target

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/proto/api_key"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhealthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

var (
	targetStr = flag.String("webdriver_target", "local", "Target that should be tested by the webdriver. Should be 'local' or 'remote'. For remote targets, you must also set the --remote_app_endpoint and --remote_sso_slug flags.")

	remoteAppEndpoint = flag.String("remote_app_endpoint", "", "For remote targets, the endpoint to reach the app (Ex. https://app.buildbuddy.dev, or https://app.buildbuddy.io).")
	remoteSSOSlug     = flag.String("remote_sso_slug", "", "For remote targets, the SSO slug to login. Self-auth must be enabled for these targets.")
)

type WebdriverTarget interface {
	AppURL() string
	SSOSlug() string
}

type localTarget struct {
	app *app.App
}

type remoteTarget struct{}

func (d *localTarget) init(t *testing.T) {
	tempDir := testfs.MakeTempDir(t)
	dataSource := "sqlite3://" + filepath.Join(tempDir, "webdriver-local-test.db")

	httpPort := testport.FindFree(t)
	gRPCPort := testport.FindFree(t)
	monitoringPort := testport.FindFree(t)

	createGroup(t, dataSource, d.SSOSlug())
	appURL := fmt.Sprintf("http://localhost:%d", httpPort)
	configPath := createSSOConfigForGroup(t, tempDir, d.SSOSlug(), appURL)

	commandArgs := []string{
		"--cache.detailed_stats_enabled=true",
		"--database.data_source=" + dataSource,
	}
	appConfig := &app.App{
		HttpPort:       httpPort,
		MonitoringPort: monitoringPort,
		GRPCPort:       gRPCPort,
	}
	d.app = buildbuddy_enterprise.RunWithConfig(t, appConfig, configPath, commandArgs...)
}

func createGroup(t *testing.T, dataSource string, groupIdentifier string) {
	// Ensure the data source used to manually create a group is the same data source used by the app during the
	// webdriver tests
	flags.Set(t, "database.data_source", dataSource)

	ctx := context.Background()
	hc := testhealthcheck.NewTestingHealthChecker()
	testEnv := real_environment.NewRealEnv(hc)
	dbh, err := db.GetConfiguredDatabase(testEnv)
	require.NoError(t, err)
	userDB, err := userdb.NewUserDB(testEnv, dbh)
	require.NoError(t, err)

	groupID, err := userDB.InsertOrUpdateGroup(ctx, &tables.Group{
		URLIdentifier: &groupIdentifier,
	})
	require.NoError(t, err)

	// Ensure API key is visible to developers, so webdriver can scrape it and pass it to builds
	caps := []api_key.ApiKey_Capability{api_key.ApiKey_CACHE_WRITE_CAPABILITY}
	_, err = userDB.CreateAPIKey(ctx, groupID, "my_api_key", caps /* visibleToDeveloper */, true)
	require.NoError(t, err)
}

func createSSOConfigForGroup(t *testing.T, rootDir string, groupIdentifier string, appURL string) string {
	configWithSSO := `
app:
  build_buddy_url: ` + appURL + `
auth:
  oauth_providers:
    - issuer_url:  "https://accounts.google.com"
      client_id: "fake_id"
      client_secret: "fake_secret"
    - issuer_url: ` + appURL + `
      client_id: "buildbuddy"
      client_secret: "secret"
      slug: "` + groupIdentifier + `"
`

	path := filepath.Join(rootDir, "config.yaml")
	err := os.WriteFile(path, []byte(configWithSSO), 0664)
	require.NoError(t, err)

	return path
}

func (d *localTarget) AppURL() string {
	return d.app.HTTPURL()
}

func (d *localTarget) SSOSlug() string {
	return "buildbuddy-ui-probers"
}

func (d *remoteTarget) AppURL() string {
	return *remoteAppEndpoint
}

func (d *remoteTarget) SSOSlug() string {
	return *remoteSSOSlug
}

func Setup(t *testing.T) WebdriverTarget {
	switch *targetStr {
	case "local":
		tar := &localTarget{}
		tar.init(t)
		return tar
	case "remote":
		return &remoteTarget{}
	default:
		require.FailNowf(t, "invalid target", "%s is an invalid target for the webdriver invocation tests.", *targetStr)
		return nil
	}
}
