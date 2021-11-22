package setup_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/assert"
)

func TestHomePage_ShowsSetupFileContents(t *testing.T) {
	app := buildbuddy.Run(t)
	wt := webtester.New(t)

	wt.Get(app.HTTPURL())

	bazelrc := wt.Find(`[data-header=".bazelrc"] .contents`).Text()

	assert.Contains(t, bazelrc, "--bes_backend="+app.GRPCAddress())
	assert.Contains(t, bazelrc, "--bes_results_url="+app.HTTPURL()+"/invocation/")
	assert.NotContains(t, bazelrc, "--remote_cache=", "Should not show remote_cache unless explicitly enabled")
}

func TestHomePage_ClickEnableCache_ShowsSetupFileContents(t *testing.T) {
	app := buildbuddy.Run(t)
	wt := webtester.New(t)

	wt.Get(app.HTTPURL())

	wt.Find("#cache").Click()

	bazelrc := wt.Find(`[data-header=".bazelrc"] .contents`).Text()

	assert.Contains(t, bazelrc, "--bes_backend="+app.GRPCAddress())
	assert.Contains(t, bazelrc, "--bes_results_url="+app.HTTPURL()+"/invocation/")
	assert.Contains(t, bazelrc, "--remote_cache="+app.GRPCAddress())
}
