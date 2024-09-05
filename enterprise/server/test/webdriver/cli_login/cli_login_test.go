package cli_login_test

import (
	"bufio"
	"context"
	"io"
	"net/url"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestCLILoginWebFlow_SingleOrg_PersonalKeysEnabled(t *testing.T) {
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	app := buildbuddy_enterprise.SetupWebTarget(t)
	wt := webtester.New(t)
	webtester.Login(wt, app)

	webtester.UpdateSelectedOrg(wt, app.HTTPURL(), "Test", "test", webtester.EnableUserOwnedAPIKeys)

	ws := testcli.NewWorkspace(t)
	cli := startLoginCommand(t, ws, app)

	// Open the browser to the URL that was printed.
	wt.Get(cli.BuildBuddyURL.String())

	// We should now be redirected to the app.
	// Since we're only a member of one org, and personal keys are enabled,
	// we should immediately be redirected back to the CLI server.
	text := wt.Find(`[debug-id="cli-login-complete"]`).Text()
	require.Contains(t, text, "CLI login succeeded")

	// Wait for the CLI command to terminate.
	err := cli.Wait()
	require.NoError(t, err)

	// Personal API key should now be written to .git/config
	apiKey := readConfiguredAPIKey(t, ws)
	checkOrgAccess(t, app, "test", apiKey)
}

func TestCLILoginWebFlow_MultipleOrgs_ChooseOrgWithoutPersonalKeysEnabled(t *testing.T) {
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	app := buildbuddy_enterprise.SetupWebTarget(t)
	wt := webtester.New(t)
	webtester.Login(wt, app)

	webtester.UpdateSelectedOrg(wt, app.HTTPURL(), "Test1", "test-1", webtester.EnableUserOwnedAPIKeys)

	// Create another org.
	webtester.CreateOrg(wt, app.HTTPURL(), "Test2", "test-2")

	// Start CLI login and open the browser to the URL it prints.
	ws := testcli.NewWorkspace(t)
	cli := startLoginCommand(t, ws, app)
	wt.Get(cli.BuildBuddyURL.String())

	// We should now be redirected to BuildBuddy UI.
	// Since we're a member of multiple orgs, we have to choose an org to
	// authorize.
	wt.Find(`select[debug-id="org"]`).SendKeys("Test2")
	wt.Find(`button[debug-id="authorize"]`).Click()

	bannerText := wt.Find(`[debug-id="cli-login-settings-banner"]`).Text()
	require.Contains(t, bannerText, "an organization administrator must enable user-owned API keys in Org details.")
	require.Contains(t, bannerText, "To log in as the organization Test2, copy one of the keys below and paste it back into the login prompt")

	wt.Find(".api-key-value-hide").Click()
	orgAPIKey := wt.Find(".api-key-value .display-value").Text()

	// Send the API key on stdin.
	cli.Stdin.Write([]byte(orgAPIKey + "\n"))

	// Wait for the CLI command to terminate.
	err := cli.Wait()
	require.NoError(t, err)

	// Personal API key should now be written to .git/config
	apiKey := readConfiguredAPIKey(t, ws)
	checkOrgAccess(t, app, "test-2", apiKey)
}

func TestCLILoginWebFlow_ZeroOrgs_CreateOrgFlow(t *testing.T) {
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	app := buildbuddy_enterprise.SetupWebTarget(t)
	wt := webtester.New(t)
	webtester.Login(wt, app)

	// Leave the only org that we're a part of.
	webtester.LeaveSelectedOrg(wt, app.HTTPURL())

	// Start CLI login and open the browser to the URL it prints.
	ws := testcli.NewWorkspace(t)
	cli := startLoginCommand(t, ws, app)
	wt.Get(cli.BuildBuddyURL.String())

	// The "Create org" flow should be shown since we aren't a member of any
	// orgs.
	webtester.SubmitOrgForm(wt, "Brand New Org", "brand-new-org", webtester.EnableUserOwnedAPIKeys)

	// Creating an org with user-owned keys enabled should be enough to complete
	// the login flow.
	text := wt.Find(`[debug-id="cli-login-complete"]`).Text()
	require.Contains(t, text, "CLI login succeeded")

	// Wait for the CLI command to terminate.
	err := cli.Wait()
	require.NoError(t, err)

	// Personal API key should now be written to .git/config
	apiKey := readConfiguredAPIKey(t, ws)
	checkOrgAccess(t, app, "brand-new-org", apiKey)
}

type loginCommand struct {
	*exec.Cmd

	Stdin         io.WriteCloser
	ServerURL     *url.URL
	BuildBuddyURL *url.URL
}

func startLoginCommand(t *testing.T, workspaceDir string, app buildbuddy_enterprise.WebTarget) *loginCommand {
	cli := testcli.Command(t, workspaceDir, "login", "--url", app.HTTPURL(), "--target", app.GRPCAddress(), "--no_launch_browser")
	stdin, err := cli.StdinPipe()
	require.NoError(t, err)
	stderr, err := cli.StderrPipe()
	require.NoError(t, err)
	t.Logf("Starting CLI login command.")
	err = cli.Start()
	require.NoError(t, err)

	// Look for the localhost URL that the CLI prints to stderr.
	s := bufio.NewScanner(stderr)
	serverURLPattern := regexp.MustCompile(`Running.*login server.*(http://.*)`)
	buildbuddyURLPattern := regexp.MustCompile(`(http://localhost:\d+/cli-login?[^\s]*)$`)
	var serverURL, buildbuddyURL *url.URL
	for s.Scan() {
		line := s.Text()
		t.Logf("[bb login] %s", line)

		if m := serverURLPattern.FindStringSubmatch(line); len(m) > 0 {
			serverURL, err = url.Parse(m[1])
			require.NoError(t, err)
		}
		if m := buildbuddyURLPattern.FindStringSubmatch(line); len(m) > 0 {
			buildbuddyURL, err = url.Parse(m[1])
			require.NoError(t, err)
		}

		if serverURL != nil && buildbuddyURL != nil {
			// Discard the remaining output so that the CLI doesn't get stuck
			// writing stderr.
			go io.Copy(io.Discard, stderr)
			break
		}
	}
	require.NoError(t, s.Err())
	return &loginCommand{
		Cmd:           cli,
		Stdin:         stdin,
		ServerURL:     serverURL,
		BuildBuddyURL: buildbuddyURL,
	}
}

func readConfiguredAPIKey(t *testing.T, repoDir string) string {
	git := exec.Command("git", "config", "--local", "buildbuddy.api-key")
	git.Dir = repoDir
	b, err := git.CombinedOutput()
	require.NoError(t, err, "git output: %q", string(b))
	return strings.TrimSpace(string(b))
}

func checkOrgAccess(t *testing.T, app buildbuddy_enterprise.WebTarget, slug, apiKey string) {
	conn, err := grpc_client.DialSimpleWithoutPooling(app.GRPCAddress())
	require.NoError(t, err)
	defer conn.Close()
	client := bbspb.NewBuildBuddyServiceClient(conn)

	// Perform an unauthenticated request to get the group ID for the slug.
	ctx := context.Background()
	res, err := client.GetGroup(ctx, &grpb.GetGroupRequest{
		UrlIdentifier: slug,
	})
	require.NoError(t, err)
	groupID := res.GetId()

	// Check that the key has access to this group's data.
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	_, err = client.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		Query:          &inpb.InvocationQuery{GroupId: groupID},
	})
	require.NoError(t, err)
}
