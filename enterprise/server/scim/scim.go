package scim

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

var (
	enableSCIM = flag.Bool("auth.enable_scim", false, "Whether or not to enable SCIM.")
)

const (
	usersPath = "/scim/Users"

	ListResponseSchema  = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
	UserResourceSchema  = "urn:ietf:params:scim:schemas:core:2.0:User"
	PatchResourceSchema = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
)

type NameResource struct {
	GivenName  string `json:"givenName"`
	FamilyName string `json:"familyName"`
}

type EmailResource struct {
	Primary bool   `json:"primary"`
	Value   string `json:"value"`
	Type    string `json:"type"`
}

type UserResource struct {
	Schemas  []string        `json:"schemas"`
	ID       string          `json:"id"`
	UserName string          `json:"userName"`
	Name     NameResource    `json:"name"`
	Emails   []EmailResource `json:"emails"`
	Active   bool            `json:"active"`
}

func newUserResource(u *tables.User) *UserResource {
	return &UserResource{
		Schemas:  []string{UserResourceSchema},
		ID:       u.UserID,
		UserName: u.Email,
		Name: NameResource{
			GivenName:  u.FirstName,
			FamilyName: u.LastName,
		},
		Emails: []EmailResource{
			{
				Primary: true,
				Value:   u.Email,
			},
		},
		Active: true,
	}
}

type ListResponseResource struct {
	Schemas      []string `json:"schemas"`
	TotalResults int      `json:"totalResults"`
	StartIndex   int      `json:"startIndex"`
	ItemsPerPage int      `json:"itemsPerPage"`
	Resources    []*UserResource
}

type OperationResource struct {
	Op    string         `json:"op"`
	Path  string         `json:"path"`
	Value map[string]any `json:"value"`
}

type PatchResource struct {
	Schemas    []string `json:"schemas"`
	Operations []OperationResource
}

type SCIMServer struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if *enableSCIM {
		env.SetSCIMService(NewSCIMServer(env))
	}
	return nil
}

func NewSCIMServer(env environment.Env) *SCIMServer {
	return &SCIMServer{
		env: env,
	}
}

type handlerFunc func(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error)

func mapErrorCode(err error) int {
	if status.IsNotFoundError(err) {
		return http.StatusNotFound
	} else if status.IsInvalidArgumentError(err) {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func (s *SCIMServer) RegisterHandlers(mux interfaces.HttpServeMux) {
	fn := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, usersPath) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		u, err := s.env.GetAuthenticator().AuthenticatedUser(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if !u.HasCapability(akpb.ApiKey_SCIM_CAPABILITY) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		g, err := s.env.GetUserDB().GetGroupByID(r.Context(), u.GetGroupID())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("could not lookup group information"))
			return
		}
		if g.SamlIdpMetadataUrl == nil || *g.SamlIdpMetadataUrl == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("SCIM API can only be used in conjunction with SAML"))
			return
		}

		var h handlerFunc
		switch r.Method {
		case "GET":
			if r.URL.Path == usersPath {
				h = s.getUsers
			} else {
				h = s.getUser
			}
		case "POST":
			h = s.createUser
		case "PUT":
			h = s.updateUser
		case "PATCH":
			h = s.patchUser
		case "DELETE":
			h = s.deleteUser
		default:
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		val, err := h(r.Context(), r, g)
		if err != nil {
			w.WriteHeader(mapErrorCode(err))
			w.Write([]byte(err.Error()))
			return
		}
		out, err := json.Marshal(val)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.Write(out)
	})
	mux.Handle("/scim/", interceptors.WrapAuthenticatedExternalHandler(s.env, fn))
}

func (s *SCIMServer) getFilteredUsers(ctx context.Context, filter string) ([]*UserResource, error) {
	filterParts := strings.Split(filter, " ")
	if len(filterParts) != 3 {
		return nil, status.InvalidArgumentErrorf("unsupported filter %q", filter)
	}
	if filterParts[0] != "userName" {
		return nil, status.InvalidArgumentErrorf("unsupported filter attribute %q", filterParts[0])
	}
	if filterParts[1] != "eq" {
		return nil, status.InvalidArgumentErrorf("unsupported filter operator %q", filterParts[1])
	}
	email, err := strconv.Unquote(filterParts[2])
	if err != nil {
		return nil, err
	}
	u, err := s.env.GetUserDB().GetUserByEmail(ctx, email)
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	return []*UserResource{newUserResource(u)}, nil
}

func (s *SCIMServer) getUsers(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	startIndex := 0
	startIndexParam := r.URL.Query().Get("startIndex")
	if startIndexParam != "" {
		v, err := strconv.Atoi(startIndexParam)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid startIndex value: %s", err)
		}
		startIndex = v - 1
		if startIndex < 0 {
			startIndex = 0
		}
	}

	count := 0
	countParam := r.URL.Query().Get("count")
	if countParam != "" {
		v, err := strconv.Atoi(countParam)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalud count value: %s", err)
		}
		count = v
		if count < 0 {
			count = 0
		}
	}

	users := []*UserResource{}
	filter := r.URL.Query().Get("filter")
	if filter == "" {
		displayUsers, err := s.env.GetUserDB().GetGroupUsers(ctx, g.GroupID, []grpb.GroupMembershipStatus{grpb.GroupMembershipStatus_MEMBER})
		if err != nil {
			return nil, err
		}
		for _, du := range displayUsers {
			u := &tables.User{
				UserID:    du.GetUser().GetUserId().GetId(),
				FirstName: du.GetUser().GetName().GetFirst(),
				LastName:  du.GetUser().GetName().GetLast(),
				Email:     du.GetUser().GetEmail(),
			}
			users = append(users, newUserResource(u))
		}
	} else {
		fu, err := s.getFilteredUsers(ctx, filter)
		if err != nil {
			return nil, err
		}
		users = fu
	}
	slices.SortFunc(users, func(a, b *UserResource) int {
		return strings.Compare(a.UserName, b.UserName)
	})
	totalResults := len(users)

	if startIndex > len(users) {
		startIndex = len(users)
	}
	users = users[startIndex:]

	if count == 0 {
		count = len(users)
	}
	if count > len(users) {
		count = len(users)
	}
	users = users[:count]

	return &ListResponseResource{
		Schemas:      []string{ListResponseSchema},
		TotalResults: totalResults,
		StartIndex:   startIndex + 1,
		ItemsPerPage: count,
		Resources:    users,
	}, nil
}

func (s *SCIMServer) getUser(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	id := path.Base(r.URL.Path)
	u, err := s.env.GetUserDB().GetUserByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return newUserResource(u), nil
}

func (s *SCIMServer) createUser(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	ur := UserResource{}
	if err := json.Unmarshal(req, &ur); err != nil {
		return nil, err
	}
	emailParts := strings.Split(ur.UserName, "@")
	if len(emailParts) != 2 {
		return nil, status.InvalidArgumentErrorf("invalid username %q", ur.UserName)
	}
	if emailParts[1] != g.OwnedDomain {
		return nil, status.InvalidArgumentErrorf("username domain %q does not match group domain %q", ur.UserName, g.OwnedDomain)
	}

	pk, err := tables.PrimaryKeyForTable("Users")
	if err != nil {
		return nil, err
	}
	entityURL := build_buddy_url.WithPath("saml/metadata?slug=" + *g.URLIdentifier)
	u := &tables.User{
		UserID:    pk,
		SubID:     fmt.Sprintf("%s/%s", entityURL, ur.UserName),
		FirstName: ur.Name.GivenName,
		LastName:  ur.Name.FamilyName,
		Email:     ur.UserName,
		Groups: []*tables.GroupRole{
			{Group: tables.Group{URLIdentifier: g.URLIdentifier}},
		},
	}
	if err := s.env.GetUserDB().InsertUser(ctx, u); err != nil {
		return nil, err
	}

	return newUserResource(u), nil
}

func (s *SCIMServer) patchUser(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	pr := PatchResource{}
	if err := json.Unmarshal(req, &pr); err != nil {
		return nil, err
	}

	id := path.Base(r.URL.Path)
	u, err := s.env.GetUserDB().GetUserByID(ctx, id)
	if err != nil {
		return nil, err
	}

	deleteUser := false
	// The only update we support in PATCH is user deletion.
	for _, op := range pr.Operations {
		if op.Op != "replace" {
			return nil, status.InvalidArgumentErrorf("unsupported operation %q", op.Op)
		}
		if op.Path != "" {
			return nil, status.InvalidArgumentErrorf("patch path not supported")
		}
		for k, v := range op.Value {
			if k != "active" {
				return nil, status.InvalidArgumentErrorf("unsupported patch attribute %q", k)
			}
			b, ok := v.(bool)
			if !ok {
				return nil, status.InvalidArgumentErrorf("expected boolean value for 'active' attribute, but got %T", v)
			}
			deleteUser = !b
		}
	}

	ur := newUserResource(u)
	if deleteUser {
		err = s.env.GetUserDB().DeleteUser(ctx, id)
		if err != nil {
			return nil, err
		}
		ur.Active = false
	}

	return ur, nil
}

func (s *SCIMServer) updateUser(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	ur := UserResource{}
	if err := json.Unmarshal(req, &ur); err != nil {
		return nil, err
	}

	id := path.Base(r.URL.Path)
	u, err := s.env.GetUserDB().GetUserByID(ctx, id)
	if err != nil {
		return nil, err
	}

	u.FirstName = ur.Name.GivenName
	u.LastName = ur.Name.FamilyName
	updatedUser := newUserResource(u)
	if !ur.Active {
		err = s.env.GetUserDB().DeleteUser(ctx, id)
		if err != nil {
			return nil, err
		}
		updatedUser.Active = false
	} else {
		return nil, status.InvalidArgumentErrorf("updating attributes not yet supported")
	}
	return updatedUser, nil
}

func (s *SCIMServer) deleteUser(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	return nil, status.UnimplementedError("delete not supported")
}
