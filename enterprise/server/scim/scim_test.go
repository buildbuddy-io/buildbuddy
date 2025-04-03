package scim_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/saml"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scim"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func getEnv(t *testing.T) *testenv.TestEnv {
	env := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, env)
	return env
}

func authUserCtx(ctx context.Context, env environment.Env, t *testing.T, userID string) context.Context {
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	ctx, err := auth.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	return ctx
}

func prepareGroup(t *testing.T, ctx context.Context, env environment.Env) (string, *tables.Group) {
	u, err := env.GetUserDB().GetUser(ctx)
	require.NoError(t, err)
	g := u.Groups[0].Group

	apiKey, err := env.GetAuthDB().CreateAPIKey(ctx, g.GroupID, "SCIM", []akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY}, false)
	require.NoError(t, err)

	g.SamlIdpMetadataUrl = "foo"

	err = env.GetDBHandle().NewQuery(ctx, "update").Update(&g)
	require.NoError(t, err)
	tu, err := env.GetUserDB().GetUser(ctx)
	require.NoError(t, err)
	require.Len(t, tu.Groups, 1, "takeOwnershipOfDomain: user must be part of exactly one group")

	gr := tu.Groups[0].Group
	if gr.URLIdentifier == "" {
		gr.URLIdentifier = strings.ToLower(gr.GroupID + "-slug")
	}
	gr.OwnedDomain = strings.Split(tu.Email, "@")[1]
	_, err = env.GetUserDB().UpdateGroup(ctx, &gr)
	require.NoError(t, err)

	return apiKey.Value, &gr
}

type testClient struct {
	t      *testing.T
	apiKey string
}

func (tc *testClient) do(method string, url string, body []byte) (int, []byte) {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	require.NoError(tc.t, err)

	req.Header[authutil.APIKeyHeader] = []string{tc.apiKey}
	rsp, err := http.DefaultClient.Do(req)
	require.NoError(tc.t, err)
	b, err := io.ReadAll(rsp.Body)
	require.NoError(tc.t, err)
	return rsp.StatusCode, b
}

func (tc *testClient) Get(url string) (int, []byte) {
	return tc.do(http.MethodGet, url, nil)
}

func (tc *testClient) Post(url string, body []byte) (int, []byte) {
	return tc.do(http.MethodPost, url, body)
}

func (tc *testClient) Patch(url string, body []byte) (int, []byte) {
	return tc.do(http.MethodPatch, url, body)
}

func (tc *testClient) Put(url string, body []byte) (int, []byte) {
	return tc.do(http.MethodPut, url, body)
}

func (tc *testClient) Delete(url string) (int, []byte) {
	return tc.do(http.MethodDelete, url, nil)
}

func verifyRole(t *testing.T, ur scim.UserResource, expectedRole string) {
	require.Equal(t, expectedRole, ur.Role)
	require.Len(t, ur.Roles, 1)
	require.Equal(t, expectedRole, ur.Roles[0].Value)
	require.Equal(t, true, ur.Roles[0].Primary)
}

func updateUserSubID(t *testing.T, ctx context.Context, udb interfaces.UserDB, userID string, g *tables.Group) {
	u, err := udb.GetUserByID(ctx, userID)
	require.NoError(t, err)
	u.SubID = saml.SubIDForUserName(u.Email, g)
	err = udb.UpdateUser(ctx, u)
	require.NoError(t, err)
}

func TestGetUsers(t *testing.T) {
	env := getEnv(t)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create first user & group.
	err := udb.InsertUser(ctx, &tables.User{
		UserID: "US100",
		SubID:  "SubID100",
		Email:  "user100@org1.io",
	})
	require.NoError(t, err)

	// Create a user in a different group.
	err = udb.InsertUser(ctx, &tables.User{
		UserID: "US999",
		SubID:  "SubID999",
		Email:  "user999@org999.io",
	})
	require.NoError(t, err)

	userCtx := authUserCtx(ctx, env, t, "US100")
	apiKey, gr := prepareGroup(t, userCtx, env)
	updateUserSubID(t, userCtx, udb, "US100", gr)

	// Add another user to the group with the same e-mail but non-matching
	// SubID prefix. This user should not be returned by the SCIM call.
	err = udb.InsertUser(ctx, &tables.User{
		UserID: "US777",
		SubID:  "SubID777",
		Email:  "user100@org1.io",
	})
	require.NoError(t, err)

	extraUsers := []*tables.User{}
	for i := 101; i < 111; i++ {
		email := fmt.Sprintf("user%d@org1.io", i)
		extraUsers = append(extraUsers, &tables.User{
			UserID: fmt.Sprintf("US%d", i),
			SubID:  saml.SubIDForUserName(email, gr),
			Email:  email,
		})
	}
	rand.Shuffle(len(extraUsers), func(i, j int) {
		a := extraUsers[i]
		extraUsers[i] = extraUsers[j]
		extraUsers[j] = a
	})

	for _, u := range extraUsers {
		err := udb.InsertUser(userCtx, u)
		require.NoError(t, err)
	}

	ss := scim.NewSCIMServer(env)
	mux := http.NewServeMux()
	ss.RegisterHandlers(mux)

	baseURL := testhttp.StartServer(t, mux).String()
	tc := &testClient{t: t, apiKey: apiKey}

	// Get users w/o filtering or pagination.
	{
		code, body := tc.Get(baseURL + "/scim/Users")
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		lr := scim.UserListResponseResource{}
		err = json.Unmarshal(body, &lr)
		require.NoError(t, err)
		require.Len(t, lr.Schemas, 1)
		require.Equal(t, scim.ListResponseSchema, lr.Schemas[0])
		require.Equal(t, 11, lr.TotalResults)
		require.Equal(t, 1, lr.StartIndex)
		require.Equal(t, 11, lr.ItemsPerPage)
		require.Len(t, lr.Resources, 11)
		for i, r := range lr.Resources {
			require.Equal(t, fmt.Sprintf("US%d", 100+i), r.ID)
			require.Equal(t, fmt.Sprintf("user%d@org1.io", 100+i), r.UserName)
			require.True(t, r.Active)
		}
	}

	// Test basic pagination
	{
		code, body := tc.Get(baseURL + "/scim/Users?startIndex=5&count=3")
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		lr := scim.UserListResponseResource{}
		err = json.Unmarshal(body, &lr)
		require.NoError(t, err)
		require.Len(t, lr.Schemas, 1)
		require.Equal(t, scim.ListResponseSchema, lr.Schemas[0])
		require.Equal(t, 11, lr.TotalResults)
		require.Equal(t, 5, lr.StartIndex)
		require.Equal(t, 3, lr.ItemsPerPage)
		require.Len(t, lr.Resources, 3)
		for i, r := range lr.Resources {
			require.Equal(t, fmt.Sprintf("US%d", 104+i), r.ID)
			require.Equal(t, fmt.Sprintf("user%d@org1.io", 104+i), r.UserName)
			require.True(t, r.Active)
		}
	}

	// Test using filter to look up a specific user.
	// There are two users with the same e-mail address but only one has a
	// SubID that matches the SAML application.
	// The query should ignore the existence of the user with the non-matching
	// SubID.
	{
		code, body := tc.Get(baseURL + "/scim/Users?filter=" + url.QueryEscape(`userName eq "user100@org1.io"`))
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		lr := scim.UserListResponseResource{}
		err = json.Unmarshal(body, &lr)
		require.NoError(t, err)
		require.Len(t, lr.Schemas, 1)
		require.Equal(t, scim.ListResponseSchema, lr.Schemas[0])
		require.Equal(t, 1, lr.TotalResults)
		require.Equal(t, 1, lr.StartIndex)
		require.Equal(t, 1, lr.ItemsPerPage)
		require.Len(t, lr.Resources, 1)

		r := lr.Resources[0]
		require.Equal(t, "US100", r.ID)
		require.Equal(t, "user100@org1.io", r.UserName)
		require.True(t, r.Active)
	}

	// Test using filter to look up a specific user that doesn't exist.
	{
		code, body := tc.Get(baseURL + "/scim/Users?filter=" + url.QueryEscape(`userName eq "user200@org1.io"`))
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		lr := scim.UserListResponseResource{}
		err = json.Unmarshal(body, &lr)
		require.NoError(t, err)
		require.Len(t, lr.Schemas, 1)
		require.Equal(t, scim.ListResponseSchema, lr.Schemas[0])
		require.Equal(t, 0, lr.TotalResults)
		require.Equal(t, 1, lr.StartIndex)
		require.Equal(t, 0, lr.ItemsPerPage)
		require.Len(t, lr.Resources, 0)
	}

	// Test using filter to attempt to lookup user in a different group.
	{
		code, body := tc.Get(baseURL + "/scim/Users?filter=" + url.QueryEscape(`userName eq "user999@org999.io"`))
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		lr := scim.UserListResponseResource{}
		err = json.Unmarshal(body, &lr)
		require.NoError(t, err)
		require.Len(t, lr.Schemas, 1)
		require.Equal(t, scim.ListResponseSchema, lr.Schemas[0])
		require.Equal(t, 0, lr.TotalResults)
		require.Equal(t, 1, lr.StartIndex)
		require.Equal(t, 0, lr.ItemsPerPage)
		require.Len(t, lr.Resources, 0)
	}

	// Test lookup by ID.
	{
		code, body := tc.Get(baseURL + "/scim/Users/US108")
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		ur := scim.UserResource{}
		err = json.Unmarshal(body, &ur)
		require.NoError(t, err)
		require.Len(t, ur.Schemas, 1)
		require.Equal(t, scim.UserResourceSchema, ur.Schemas[0])
		require.Equal(t, "US108", ur.ID)
		require.Equal(t, "user108@org1.io", ur.UserName)
		require.True(t, ur.Active)
	}

	// Test lookup by ID for a non-existent user.
	{
		code, body := tc.Get(baseURL + "/scim/Users/US200")
		require.Equal(tc.t, http.StatusNotFound, code, "body: %s", string(body))
	}

	// Test lookup by ID for a different group.
	{
		code, body := tc.Get(baseURL + "/scim/Users/US999")
		require.Equal(tc.t, http.StatusNotFound, code, "body: %s", string(body))
	}
}

func TestCreateUser(t *testing.T) {
	env := getEnv(t)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create first user & group.
	err := udb.InsertUser(ctx, &tables.User{
		UserID: "US100",
		SubID:  "SubID100",
		Email:  "user100@org1.io",
	})
	require.NoError(t, err)

	userCtx := authUserCtx(ctx, env, t, "US100")
	apiKey, group := prepareGroup(t, userCtx, env)

	ss := scim.NewSCIMServer(env)
	mux := http.NewServeMux()
	ss.RegisterHandlers(mux)

	baseURL := testhttp.StartServer(t, mux).String()
	tc := &testClient{t: t, apiKey: apiKey}

	// Create developer user.
	{
		newUser := &scim.UserResource{
			Schemas:  []string{scim.UserResourceSchema},
			UserName: "user500@org1.io",
			Name: scim.NameResource{
				GivenName:  "User",
				FamilyName: "Doe",
			},
			Emails: []scim.EmailResource{
				{
					Primary: true,
					Value:   "user500@org1.io",
				},
			},
			Active: true,
		}
		body, err := json.Marshal(newUser)
		require.NoError(t, err)

		code, body := tc.Post(baseURL+"/scim/Users", body)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		createdUser := scim.UserResource{}
		err = json.Unmarshal(body, &createdUser)
		require.NoError(t, err)
		require.Equal(t, "User", createdUser.Name.GivenName)
		require.Equal(t, "Doe", createdUser.Name.FamilyName)
		require.Equal(t, "user500@org1.io", createdUser.UserName)
		require.True(t, createdUser.Active)

		code, body = tc.Get(baseURL + "/scim/Users/" + createdUser.ID)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		ur := scim.UserResource{}
		err = json.Unmarshal(body, &ur)
		require.NoError(t, err)
		require.Len(t, ur.Schemas, 1)
		require.Equal(t, scim.UserResourceSchema, ur.Schemas[0])
		require.Equal(t, createdUser.ID, ur.ID)
		require.Equal(t, "User", ur.Name.GivenName)
		require.Equal(t, "Doe", ur.Name.FamilyName)
		require.Equal(t, "user500@org1.io", ur.UserName)
		verifyRole(t, ur, role.Developer.String())
		require.True(t, ur.Active)

		u, err := udb.GetUserByID(userCtx, createdUser.ID)
		require.NoError(t, err)
		require.Equal(t, "http://localhost:8080/saml/metadata?slug=gr100-slug/user500@org1.io", u.SubID)
	}

	// Create admin user.
	user501ID := ""
	{
		newUser := &scim.UserResource{
			Schemas:  []string{scim.UserResourceSchema},
			UserName: "user501@org1.io",
			Role:     role.Admin.String(),
			Name: scim.NameResource{
				GivenName:  "Foo",
				FamilyName: "Bar",
			},
			Emails: []scim.EmailResource{
				{
					Primary: true,
					Value:   "user501@org1.io",
				},
			},
			Active: true,
		}
		body, err := json.Marshal(newUser)
		require.NoError(t, err)

		code, body := tc.Post(baseURL+"/scim/Users", body)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		createdUser := scim.UserResource{}
		err = json.Unmarshal(body, &createdUser)
		require.NoError(t, err)
		require.Equal(t, "Foo", createdUser.Name.GivenName)
		require.Equal(t, "Bar", createdUser.Name.FamilyName)
		require.Equal(t, "user501@org1.io", createdUser.UserName)
		verifyRole(t, createdUser, role.Admin.String())
		require.True(t, createdUser.Active)

		code, body = tc.Get(baseURL + "/scim/Users/" + createdUser.ID)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		ur := scim.UserResource{}
		err = json.Unmarshal(body, &ur)
		require.NoError(t, err)
		require.Len(t, ur.Schemas, 1)
		require.Equal(t, scim.UserResourceSchema, ur.Schemas[0])
		require.Equal(t, createdUser.ID, ur.ID)
		require.Equal(t, "Foo", ur.Name.GivenName)
		require.Equal(t, "Bar", ur.Name.FamilyName)
		require.Equal(t, "user501@org1.io", ur.UserName)
		verifyRole(t, ur, role.Admin.String())
		require.True(t, ur.Active)

		user501ID = createdUser.ID
	}

	// Remove a user manually from the SCIM-managed group and try creating the
	// user through the SCIM API. The user should be added to the target group.
	{
		err = udb.UpdateGroupUsers(userCtx, group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{{
			UserId:           &uidpb.UserId{Id: user501ID},
			MembershipAction: grpb.UpdateGroupUsersRequest_Update_REMOVE,
		}})
		require.NoError(t, err)

		newUser := &scim.UserResource{
			Schemas:  []string{scim.UserResourceSchema},
			UserName: "user501@org1.io",
			Name: scim.NameResource{
				GivenName:  "User",
				FamilyName: "Doe",
			},
			Emails: []scim.EmailResource{
				{
					Primary: true,
					Value:   "user501@org1.io",
				},
			},
			Active: true,
		}
		body, err := json.Marshal(newUser)
		require.NoError(t, err)

		code, body := tc.Post(baseURL+"/scim/Users", body)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		createdUser := scim.UserResource{}
		err = json.Unmarshal(body, &createdUser)
		require.NoError(t, err)
		require.Equal(t, "User", createdUser.Name.GivenName)
		require.Equal(t, "Doe", createdUser.Name.FamilyName)
		require.Equal(t, "user501@org1.io", createdUser.UserName)
		require.True(t, createdUser.Active)

		code, body = tc.Get(baseURL + "/scim/Users/" + createdUser.ID)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		ur := scim.UserResource{}
		err = json.Unmarshal(body, &ur)
		require.NoError(t, err)
		require.Len(t, ur.Schemas, 1)
		require.Equal(t, scim.UserResourceSchema, ur.Schemas[0])
		require.Equal(t, createdUser.ID, ur.ID)
		require.Equal(t, "User", ur.Name.GivenName)
		require.Equal(t, "Doe", ur.Name.FamilyName)
		require.Equal(t, "user501@org1.io", ur.UserName)
		verifyRole(t, ur, role.Developer.String())
		require.True(t, ur.Active)

		u, err := udb.GetUserByID(userCtx, createdUser.ID)
		require.NoError(t, err)
		require.Equal(t, "http://localhost:8080/saml/metadata?slug=gr100-slug/user501@org1.io", u.SubID)
	}
}

func TestDeleteUser(t *testing.T) {
	env := getEnv(t)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create first user & group.
	err := udb.InsertUser(ctx, &tables.User{
		UserID: "US100",
		SubID:  "SubID100",
		Email:  "user100@org1.io",
	})
	require.NoError(t, err)
	// Create a user in a different group.
	err = udb.InsertUser(ctx, &tables.User{
		UserID: "US999",
		SubID:  "SubID999",
		Email:  "user999@org999.io",
	})
	require.NoError(t, err)

	userCtx := authUserCtx(ctx, env, t, "US100")
	apiKey, _ := prepareGroup(t, userCtx, env)

	// Deletion victims.
	err = udb.InsertUser(userCtx, &tables.User{
		UserID: "US101",
		SubID:  "SubID101",
		Email:  "user101@org1.io",
	})
	require.NoError(t, err)
	err = udb.InsertUser(userCtx, &tables.User{
		UserID: "US102",
		SubID:  "SubID102",
		Email:  "user102@org1.io",
	})
	require.NoError(t, err)
	err = udb.InsertUser(userCtx, &tables.User{
		UserID: "US103",
		SubID:  "SubID103",
		Email:  "user103@org1.io",
	})
	require.NoError(t, err)
	err = udb.InsertUser(userCtx, &tables.User{
		UserID: "US104",
		SubID:  "SubID104",
		Email:  "user104@org1.io",
	})
	require.NoError(t, err)

	ss := scim.NewSCIMServer(env)
	mux := http.NewServeMux()
	ss.RegisterHandlers(mux)

	baseURL := testhttp.StartServer(t, mux).String()
	tc := &testClient{t: t, apiKey: apiKey}

	// Delete user US101 using a PATCH request setting active to false.
	{
		newUser := &scim.PatchResource{
			Schemas: []string{scim.PatchResourceSchema},
			Operations: []scim.OperationResource{
				{
					Op: "replace",
					Value: map[string]any{
						"active": false,
					},
				},
			},
		}
		body, err := json.Marshal(newUser)
		require.NoError(t, err)
		code, body := tc.Patch(baseURL+"/scim/Users/US101", body)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		updatedUser := scim.UserResource{}
		err = json.Unmarshal(body, &updatedUser)
		require.NoError(t, err)
		require.False(t, updatedUser.Active)
		_, err = udb.GetUserByID(userCtx, "US101")
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err))
	}

	// Delete user US102 using a PUT request setting active to false.
	{
		req := &scim.UserResource{
			Schemas: []string{scim.UserResourceSchema},
			Active:  false,
		}
		body, err := json.Marshal(req)
		require.NoError(t, err)
		code, body := tc.Put(baseURL+"/scim/Users/US102", body)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		updatedUser := scim.UserResource{}
		err = json.Unmarshal(body, &updatedUser)
		require.NoError(t, err)
		require.False(t, updatedUser.Active)
		_, err = udb.GetUserByID(userCtx, "US102")
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err))
	}

	// Delete user US103 using a PATCH request setting active to "false" and
	// also setting patch path. Azure AD incorrectly sends the active field
	// as a string instead of a boolean.
	{
		newUser := &scim.PatchResource{
			Schemas: []string{scim.PatchResourceSchema},
			Operations: []scim.OperationResource{
				{
					Op:    "RePlAcE",
					Path:  scim.ActiveAttribute,
					Value: "false",
				},
			},
		}
		body, err := json.Marshal(newUser)
		require.NoError(t, err)
		code, body := tc.Patch(baseURL+"/scim/Users/US103", body)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		updatedUser := scim.UserResource{}
		err = json.Unmarshal(body, &updatedUser)
		require.NoError(t, err)
		require.False(t, updatedUser.Active)
		_, err = udb.GetUserByID(userCtx, "US103")
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err))
	}

	// Delete user US104 using a DELETE request.
	{
		code, body := tc.Delete(baseURL + "/scim/Users/US104")
		require.Equal(tc.t, http.StatusNoContent, code, "body: %s", string(body))
		require.NoError(t, err)
		_, err = udb.GetUserByID(userCtx, "US104")
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err))
	}

	// Deleting a user in a different group shouldn't work.
	{
		newUser := &scim.PatchResource{
			Schemas: []string{scim.PatchResourceSchema},
			Operations: []scim.OperationResource{
				{
					Op: "replace",
					Value: map[string]any{
						"active": false,
					},
				},
			},
		}
		body, err := json.Marshal(newUser)
		require.NoError(t, err)
		code, body := tc.Patch(baseURL+"/scim/Users/US999", body)
		require.Equal(tc.t, http.StatusNotFound, code, "body: %s", string(body))
	}
}

func TestUpdateUser(t *testing.T) {
	env := getEnv(t)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create first user & group.
	err := udb.InsertUser(ctx, &tables.User{
		UserID: "US100",
		SubID:  "SubID100",
		Email:  "user100@org1.io",
	})
	require.NoError(t, err)

	userCtx := authUserCtx(ctx, env, t, "US100")
	apiKey, _ := prepareGroup(t, userCtx, env)

	ss := scim.NewSCIMServer(env)
	mux := http.NewServeMux()
	ss.RegisterHandlers(mux)

	baseURL := testhttp.StartServer(t, mux).String()
	tc := &testClient{t: t, apiKey: apiKey}

	// Update user using PUT request.
	req := &scim.UserResource{
		Schemas: []string{scim.UserResourceSchema},
		Name: scim.NameResource{
			GivenName:  "Givy",
			FamilyName: "Famy",
		},
		Active:   true,
		UserName: "puttest@example.domain",
	}
	body, err := json.Marshal(req)
	require.NoError(t, err)
	code, body := tc.Put(baseURL+"/scim/Users/US100", body)
	require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
	updatedUser := scim.UserResource{}
	err = json.Unmarshal(body, &updatedUser)
	require.NoError(t, err)
	require.True(t, updatedUser.Active)
	require.Equal(t, "Givy", updatedUser.Name.GivenName)
	require.Equal(t, "Famy", updatedUser.Name.FamilyName)
	require.Equal(t, "puttest@example.domain", updatedUser.UserName)

	// Look up updated user.
	code, body = tc.Get(baseURL + "/scim/Users/US100")
	require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
	updatedUser = scim.UserResource{}
	err = json.Unmarshal(body, &updatedUser)
	require.NoError(t, err)
	require.True(t, updatedUser.Active)
	require.Equal(t, "Givy", updatedUser.Name.GivenName)
	require.Equal(t, "Famy", updatedUser.Name.FamilyName)
	require.Equal(t, "puttest@example.domain", updatedUser.UserName)
	verifyRole(t, updatedUser, role.Developer.String())

	// Verify that SubID was updated to reflect the new email.
	u, err := udb.GetUserByID(userCtx, updatedUser.ID)
	require.NoError(t, err)
	require.Equal(t, "http://localhost:8080/saml/metadata?slug=gr100-slug/puttest@example.domain", u.SubID)

	// Update user using PATCH request.
	{
		patchReq := &scim.PatchResource{
			Operations: []scim.OperationResource{
				{
					Op:    "replace",
					Path:  scim.FamilyNameAttribute,
					Value: "Fam",
				},
				{
					Op:    "replace",
					Path:  scim.GivenNameAttribute,
					Value: "Gov",
				},
				{
					Op:    "replace",
					Path:  scim.UserNameAttribute,
					Value: "somenewemail@example.domain",
				},
			},
			Schemas: []string{scim.PatchResourceSchema},
		}
		body, err = json.Marshal(patchReq)
		require.NoError(t, err)
		code, body = tc.Patch(baseURL+"/scim/Users/US100", body)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		updatedUser = scim.UserResource{}
		err = json.Unmarshal(body, &updatedUser)
		require.NoError(t, err)
		require.True(t, updatedUser.Active)
		require.Equal(t, "Gov", updatedUser.Name.GivenName)
		require.Equal(t, "Fam", updatedUser.Name.FamilyName)
		require.Equal(t, "somenewemail@example.domain", updatedUser.UserName)

		// Look up patched user.
		code, body = tc.Get(baseURL + "/scim/Users/US100")
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		updatedUser = scim.UserResource{}
		err = json.Unmarshal(body, &updatedUser)
		require.NoError(t, err)
		require.True(t, updatedUser.Active)
		require.Equal(t, "Gov", updatedUser.Name.GivenName)
		require.Equal(t, "Fam", updatedUser.Name.FamilyName)
		require.Equal(t, "somenewemail@example.domain", updatedUser.UserName)
		verifyRole(t, updatedUser, role.Developer.String())

		// Verify that SubID was updated to reflect the new email.
		u, err = udb.GetUserByID(userCtx, updatedUser.ID)
		require.NoError(t, err)
		require.Equal(t, "http://localhost:8080/saml/metadata?slug=gr100-slug/somenewemail@example.domain", u.SubID)
	}

	// Update the user using PATCH request with 'add' operation.
	// An upstream system may send these instead of 'replace' if an attribute,
	// like name, is empty.
	{
		newFamilyName := "Stark"
		newGivenName := "Arya"
		newEmail := "arya@example.domain"
		patchReq := &scim.PatchResource{
			Operations: []scim.OperationResource{
				{
					Op:    "add",
					Path:  scim.FamilyNameAttribute,
					Value: newFamilyName,
				},
				{
					Op:    "add",
					Path:  scim.GivenNameAttribute,
					Value: newGivenName,
				},
				{
					Op:    "add",
					Path:  scim.UserNameAttribute,
					Value: newEmail,
				},
			},
			Schemas: []string{scim.PatchResourceSchema},
		}
		body, err = json.Marshal(patchReq)
		require.NoError(t, err)
		code, body = tc.Patch(baseURL+"/scim/Users/US100", body)
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		updatedUser = scim.UserResource{}
		err = json.Unmarshal(body, &updatedUser)
		require.NoError(t, err)
		require.True(t, updatedUser.Active)
		require.Equal(t, newGivenName, updatedUser.Name.GivenName)
		require.Equal(t, newFamilyName, updatedUser.Name.FamilyName)
		require.Equal(t, newEmail, updatedUser.UserName)

		// Look up patched user.
		code, body = tc.Get(baseURL + "/scim/Users/US100")
		require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
		updatedUser = scim.UserResource{}
		err = json.Unmarshal(body, &updatedUser)
		require.NoError(t, err)
		require.True(t, updatedUser.Active)
		require.Equal(t, newGivenName, updatedUser.Name.GivenName)
		require.Equal(t, newFamilyName, updatedUser.Name.FamilyName)
		require.Equal(t, newEmail, updatedUser.UserName)
		verifyRole(t, updatedUser, role.Developer.String())

		// Verify that SubID was updated to reflect the new email.
		u, err = udb.GetUserByID(userCtx, updatedUser.ID)
		require.NoError(t, err)
		require.Equal(t, "http://localhost:8080/saml/metadata?slug=gr100-slug/"+newEmail, u.SubID)
	}

	// Promote user to admin.
	req.Role = role.Admin.String()
	body, err = json.Marshal(req)
	require.NoError(t, err)
	code, _ = tc.Put(baseURL+"/scim/Users/US100", body)
	require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))

	// Verify they have the admin role.
	code, body = tc.Get(baseURL + "/scim/Users/US100")
	require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
	updatedUser = scim.UserResource{}
	err = json.Unmarshal(body, &updatedUser)
	require.NoError(t, err)
	verifyRole(t, updatedUser, role.Admin.String())

	// If role is not set, it should default to Developer.
	req.Role = ""
	body, err = json.Marshal(req)
	require.NoError(t, err)
	code, _ = tc.Put(baseURL+"/scim/Users/US100", body)
	require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))

	// Verify they have the developer role.
	code, body = tc.Get(baseURL + "/scim/Users/US100")
	require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
	updatedUser = scim.UserResource{}
	err = json.Unmarshal(body, &updatedUser)
	require.NoError(t, err)
	verifyRole(t, updatedUser, role.Developer.String())

	// Promote user to Admin using patch request.
	patchReq := &scim.PatchResource{
		Operations: []scim.OperationResource{
			{
				Op:    "replace",
				Path:  scim.RoleAttribute,
				Value: role.Admin.String(),
			},
		},
		Schemas: []string{scim.PatchResourceSchema},
	}
	body, err = json.Marshal(patchReq)
	require.NoError(t, err)
	code, body = tc.Patch(baseURL+"/scim/Users/US100", body)
	require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))

	// Verify they have the admin role.
	code, body = tc.Get(baseURL + "/scim/Users/US100")
	require.Equal(tc.t, http.StatusOK, code, "body: %s", string(body))
	updatedUser = scim.UserResource{}
	err = json.Unmarshal(body, &updatedUser)
	require.NoError(t, err)
	verifyRole(t, updatedUser, role.Admin.String())
}
