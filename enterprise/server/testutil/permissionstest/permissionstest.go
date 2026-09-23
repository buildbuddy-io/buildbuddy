// Package permissionstest provides a real-app fixture with several users and
// organizations for end-to-end permission tests.
package permissionstest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testoidc"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

const (
	AdminName     = "admin"
	DeveloperName = "developer"
	WriterName    = "writer"
	ReaderName    = "reader"
	OutsiderName  = "outsider"
	DualName      = "dual"
	GrouplessName = "groupless"
)

// User is an OIDC identity backed by a pre-seeded BuildBuddy user row.
type User struct {
	Subject string
	Email   string
	ID      string
}

// Fixture owns a real enterprise app, OIDC provider, and pre-seeded users and
// organizations. Every call to Login creates an independent cookie jar.
type Fixture struct {
	App  *app.App
	IDP  *testoidc.Provider
	OrgA string
	OrgB string

	Users map[string]*User
}

// Client makes BuildBuddyService protolet RPCs using session cookies, an API
// key, or no credentials. APIKey clients are created without session cookies.
type Client struct {
	App            *app.App
	HTTPClient     *http.Client
	RequestContext *ctxpb.RequestContext
	APIKey         string
}

// RPCError is returned when a protolet RPC fails. It preserves both the HTTP
// status and the gRPC status code serialized by the protolet handler.
type RPCError struct {
	HTTPStatus int
	Code       codes.Code
	Message    string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.HTTPStatus, e.Message)
}

func (e *RPCError) GRPCStatus() *status.Status {
	return status.New(e.Code, e.Message)
}

var protoletErrorPattern = regexp.MustCompile(`^rpc error: code = ([A-Za-z]+) desc = (.*)$`)

// New starts a real enterprise server with self-auth disabled and a test OIDC
// provider configured, then directly seeds its SQLite DB with two orgs and the
// standard permission-test identities. Extra server flags override defaults
// for explicit deployment/cache-policy test variants.
func New(t *testing.T, extraArgs ...string) *Fixture {
	t.Helper()

	idp := testoidc.Start(t)
	users := map[string]*User{}
	for i, name := range []string{
		AdminName,
		DeveloperName,
		WriterName,
		ReaderName,
		OutsiderName,
		DualName,
		GrouplessName,
	} {
		u := &User{
			Subject: "permissions-" + name,
			Email:   name + "@permissions.test",
			ID:      fmt.Sprintf("USPERMISSIONS%02d", i+1),
		}
		idp.AddUser(u.Subject, u.Email)
		users[name] = u
	}

	providerJSON, err := json.Marshal([]map[string]string{{
		"issuer_url":    idp.IssuerURL(),
		"client_id":     testoidc.ClientID,
		"client_secret": testoidc.ClientSecret,
	}})
	require.NoError(t, err)

	args := append([]string{
		"--auth.enable_self_auth=false",
		"--auth.enable_anonymous_usage=true",
		"--auth.oauth_providers=" + string(providerJSON),
		"--http.client.allow_localhost=true",
		"--app.user_owned_keys_enabled=true",
	}, extraArgs...)
	a := buildbuddy_enterprise.RunWithConfig(
		t,
		buildbuddy_enterprise.DefaultAppConfig(t),
		buildbuddy_enterprise.NoAuthConfig,
		args...,
	)

	f := &Fixture{
		App:   a,
		IDP:   idp,
		OrgA:  "GRPERMISSIONSA",
		OrgB:  "GRPERMISSIONSB",
		Users: users,
	}
	f.seed(t)
	return f
}

func (f *Fixture) seed(t *testing.T) {
	t.Helper()
	db := f.App.DB()

	groups := []*tables.Group{
		{
			GroupID:                     f.OrgA,
			UserID:                      f.Users[AdminName].ID,
			Name:                        "Permissions organization A",
			URLIdentifier:               "permissions-org-a",
			UserOwnedKeysEnabled:        true,
			BotSuggestionsEnabled:       true,
			DeveloperOrgCreationEnabled: true,
			Status:                      grpb.Group_FREE_TIER_GROUP_STATUS,
		},
		{
			GroupID:                     f.OrgB,
			UserID:                      f.Users[OutsiderName].ID,
			Name:                        "Permissions organization B",
			URLIdentifier:               "permissions-org-b",
			UserOwnedKeysEnabled:        true,
			BotSuggestionsEnabled:       true,
			DeveloperOrgCreationEnabled: true,
			Status:                      grpb.Group_FREE_TIER_GROUP_STATUS,
		},
	}
	for _, g := range groups {
		require.NoError(t, db.Create(g).Error)
	}

	for name, u := range f.Users {
		require.NoError(t, db.Create(&tables.User{
			UserID:    u.ID,
			SubID:     f.IDP.IssuerURL() + "/" + u.Subject,
			FirstName: name,
			Email:     u.Email,
		}).Error)
	}

	memberships := []*tables.UserGroup{
		{UserUserID: f.Users[AdminName].ID, GroupGroupID: f.OrgA, Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: f.Users[DeveloperName].ID, GroupGroupID: f.OrgA, Role: uint32(role.Developer), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: f.Users[WriterName].ID, GroupGroupID: f.OrgA, Role: uint32(role.Writer), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: f.Users[ReaderName].ID, GroupGroupID: f.OrgA, Role: uint32(role.Reader), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: f.Users[DualName].ID, GroupGroupID: f.OrgA, Role: uint32(role.Reader), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: f.Users[OutsiderName].ID, GroupGroupID: f.OrgB, Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: f.Users[DualName].ID, GroupGroupID: f.OrgB, Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
	}
	for _, ug := range memberships {
		require.NoError(t, db.Create(ug).Error)
	}
}

// LoginURL starts the app's OIDC login flow and redirects back to the app root.
func (f *Fixture) LoginURL() string {
	return fmt.Sprintf(
		"%s/login/?issuer_url=%s&redirect_url=%s",
		f.App.HTTPURL(),
		url.QueryEscape(f.IDP.IssuerURL()),
		url.QueryEscape(f.App.HTTPURL()),
	)
}

// Login completes the provider's HTML form flow and returns a client carrying
// the resulting app auth cookies.
func (f *Fixture) Login(t *testing.T, user *User) *Client {
	t.Helper()
	require.NotNil(t, user)

	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	httpClient := &http.Client{Jar: jar, Timeout: 30 * time.Second}

	res, err := httpClient.Get(f.LoginURL())
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	action, form := authorizationForm(t, res)
	form.Set("user", user.Subject)
	postRes, err := httpClient.PostForm(action, form)
	require.NoError(t, err)
	defer postRes.Body.Close()
	_, err = io.Copy(io.Discard, postRes.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, postRes.StatusCode)

	appURL, err := url.Parse(f.App.HTTPURL())
	require.NoError(t, err)
	var hasAuthorizationCookie bool
	for _, c := range jar.Cookies(appURL) {
		if c.Name == "Authorization" && c.Value != "" {
			hasAuthorizationCookie = true
		}
	}
	require.True(t, hasAuthorizationCookie, "OIDC login did not set the app authorization cookie")

	client := &Client{
		App:        f.App,
		HTTPClient: httpClient,
		RequestContext: &ctxpb.RequestContext{
			UserId: &uidpb.UserId{Id: user.ID},
		},
	}
	// A cookie alone does not prove authentication. Every negative permissions
	// test must first establish a successful RPC as the intended identity.
	identity := &uspb.GetUserResponse{}
	require.NoError(t, client.RPC("GetUser", &uspb.GetUserRequest{RequestContext: client.RequestContext}, identity))
	require.Equal(t, user.ID, identity.GetDisplayUser().GetUserId().GetId())
	require.Equal(t, user.Email, identity.GetDisplayUser().GetEmail())
	return client
}

// AnonymousClient returns a client with neither cookies nor an API key.
func (f *Fixture) AnonymousClient() *Client {
	return &Client{
		App:            f.App,
		HTTPClient:     &http.Client{Timeout: 30 * time.Second},
		RequestContext: &ctxpb.RequestContext{},
	}
}

// APIKeyClient returns a client authenticated solely by the supplied API key.
func (f *Fixture) APIKeyClient(key string) *Client {
	client := f.AnonymousClient()
	client.APIKey = key
	return client
}

func authorizationForm(t *testing.T, res *http.Response) (string, url.Values) {
	t.Helper()
	doc, err := html.Parse(res.Body)
	require.NoError(t, err)

	form := url.Values{}
	action := ""
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "form" && action == "" {
			for _, a := range n.Attr {
				if a.Key == "action" {
					action = a.Val
				}
			}
			var collect func(*html.Node)
			collect = func(child *html.Node) {
				if child.Type == html.ElementNode && child.Data == "input" {
					name, value := "", ""
					for _, a := range child.Attr {
						switch a.Key {
						case "name":
							name = a.Val
						case "value":
							value = a.Val
						}
					}
					if name != "" {
						form.Set(name, value)
					}
				}
				for c := child.FirstChild; c != nil; c = c.NextSibling {
					collect(c)
				}
			}
			collect(n)
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	require.NotEmpty(t, action, "OIDC authorization page did not contain a form")
	actionURL, err := res.Request.URL.Parse(action)
	require.NoError(t, err)
	return actionURL.String(), form
}

// RPC makes a BuildBuddyService request over HTTP with this client's credentials.
func (c *Client) RPC(method string, req proto.Message, rsp proto.Message) error {
	var body []byte
	var err error
	if req != nil {
		body, err = proto.Marshal(req)
		if err != nil {
			return err
		}
	}

	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/rpc/BuildBuddyService/%s", c.App.HTTPURL(), method), bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/proto")
	if c.APIKey != "" {
		httpReq.Header.Set("x-buildbuddy-api-key", c.APIKey)
	}
	httpRsp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpRsp.Body.Close()
	rspBytes, err := io.ReadAll(httpRsp.Body)
	if err != nil {
		return err
	}
	if httpRsp.StatusCode >= http.StatusBadRequest {
		message := strings.TrimSpace(string(rspBytes))
		code := codes.Unknown
		if match := protoletErrorPattern.FindStringSubmatch(message); match != nil {
			if parsed, ok := codesByName[match[1]]; ok {
				code = parsed
			}
			message = match[2]
		}
		return &RPCError{HTTPStatus: httpRsp.StatusCode, Code: code, Message: message}
	}
	if rsp != nil {
		return proto.Unmarshal(rspBytes, rsp)
	}
	return nil
}

var codesByName = map[string]codes.Code{
	"Canceled":           codes.Canceled,
	"Unknown":            codes.Unknown,
	"InvalidArgument":    codes.InvalidArgument,
	"DeadlineExceeded":   codes.DeadlineExceeded,
	"NotFound":           codes.NotFound,
	"AlreadyExists":      codes.AlreadyExists,
	"PermissionDenied":   codes.PermissionDenied,
	"ResourceExhausted":  codes.ResourceExhausted,
	"FailedPrecondition": codes.FailedPrecondition,
	"Aborted":            codes.Aborted,
	"OutOfRange":         codes.OutOfRange,
	"Unimplemented":      codes.Unimplemented,
	"Internal":           codes.Internal,
	"Unavailable":        codes.Unavailable,
	"DataLoss":           codes.DataLoss,
	"Unauthenticated":    codes.Unauthenticated,
}
