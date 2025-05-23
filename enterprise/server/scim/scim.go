package scim

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/saml"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

var (
	enableSCIM = flag.Bool("auth.enable_scim", false, "Whether or not to enable SCIM.")
)

const (
	usersPath = "/scim/Users"

	ListResponseSchema  = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
	UserResourceSchema  = "urn:ietf:params:scim:schemas:core:2.0:User"
	PatchResourceSchema = "urn:ietf:params:scim:api:messages:2.0:PatchOp"

	ActiveAttribute     = "active"
	GivenNameAttribute  = "name.givenName"
	FamilyNameAttribute = "name.familyName"
	UserNameAttribute   = "userName"
	RoleAttribute       = `roles[primary eq "True"].value`
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

type RoleResource struct {
	Primary bool   `json:"primary"`
	Value   string `json:"value"`
}

type UserResource struct {
	Schemas  []string        `json:"schemas"`
	ID       string          `json:"id"`
	UserName string          `json:"userName"`
	Name     NameResource    `json:"name"`
	Emails   []EmailResource `json:"emails"`
	Active   bool            `json:"active"`
	// We map the user role in two different ways to be able to make both Okta
	// and Azure AD happy. For simple mappings, Azure AD only supports a list of
	// complex types and Okta does not support lists of complex types.
	// https://devforum.okta.com/t/okta-support-for-complex-json-schema-types/1285
	// Each provider will only look at the attribute it's expecting.
	Role  string         `json:"role"`
	Roles []RoleResource `json:"roles"`
}

func newUserResource(u *tables.User, authGroup *tables.Group) (*UserResource, error) {
	userRole := ""
	for _, g := range u.Groups {
		if g.Group.GroupID == authGroup.GroupID {
			userRole = role.Role(g.Role).String()
			if userRole == "" {
				return nil, status.InternalErrorf("unhandled role: %d", g.Role)
			}
		}
	}
	if userRole == "" {
		return nil, status.InternalErrorf("could not determine user role")
	}

	return &UserResource{
		Schemas:  []string{UserResourceSchema},
		ID:       u.UserID,
		UserName: u.Email,
		Role:     userRole,
		Roles: []RoleResource{
			{Primary: true, Value: userRole},
		},
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
	}, nil
}

type GroupMemberResource struct {
	Value string `json:"value"`
}

type GroupResource struct {
	Schemas     []string              `json:"schemas"`
	ID          string                `json:"id"`
	DisplayName string                `json:"displayName"`
	Members     []GroupMemberResource `json:"members,omitempty"`
}

func newGroupResource(g *tables.Group) *GroupResource {
	return &GroupResource{
		Schemas:     []string{UserResourceSchema},
		ID:          g.GroupID,
		DisplayName: g.Name,
	}
}

type UserListResponseResource struct {
	Schemas      []string        `json:"schemas"`
	TotalResults int             `json:"totalResults"`
	StartIndex   int             `json:"startIndex"`
	ItemsPerPage int             `json:"itemsPerPage"`
	Resources    []*UserResource `json:"resources,omitempty"`
}

type GroupListResponseResource struct {
	Schemas      []string         `json:"schemas"`
	TotalResults int              `json:"totalResults"`
	StartIndex   int              `json:"startIndex"`
	ItemsPerPage int              `json:"itemsPerPage"`
	Resources    []*GroupResource `json:"resources,omitempty"`
}

type OperationResource struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
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

func (s *SCIMServer) handleRequest(w http.ResponseWriter, r *http.Request, handler handlerFunc) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if !u.HasCapability(cappb.Capability_ORG_ADMIN) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	g, err := s.env.GetUserDB().GetGroupByID(r.Context(), u.GetGroupID())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("could not lookup group information"))
		return
	}
	if g.SamlIdpMetadataUrl == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("SCIM API can only be used in conjunction with SAML"))
		return
	}

	log.CtxInfof(r.Context(), "Handling request for group %s: %s %s", g.GroupID, r.Method, r.URL.RequestURI())

	val, err := handler(r.Context(), r, g)
	if err != nil {
		log.CtxWarningf(r.Context(), "SCIM request %s %q failed: %s", r.Method, r.RequestURI, err)
		w.WriteHeader(mapErrorCode(err))
		w.Write([]byte(err.Error()))
		return
	}
	if val == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	out, err := json.Marshal(val)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(out)
}

func (s *SCIMServer) getRequestHandler(r *http.Request) (handlerFunc, error) {
	if strings.HasPrefix(r.URL.Path, usersPath) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path == usersPath {
				return s.getUsers, nil
			} else {
				return s.getUser, nil
			}
		case http.MethodPost:
			return s.createUser, nil
		case http.MethodPut:
			return s.updateUser, nil
		case http.MethodPatch:
			return s.patchUser, nil
		case http.MethodDelete:
			return s.deleteUser, nil
		}
	}

	return nil, status.NotFoundError("not found")
}

func (s *SCIMServer) RegisterHandlers(mux interfaces.HttpServeMux) {
	fn := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, err := s.getRequestHandler(r)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.handleRequest(w, r, h)
	})
	mux.Handle("/scim/", interceptors.WrapAuthenticatedExternalHandler(s.env, fn))
}

func (s *SCIMServer) getFilteredUsers(ctx context.Context, g *tables.Group, filter string) ([]*UserResource, error) {
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
	u, err := s.env.GetAuthDB().LookupUserFromSubID(ctx, saml.SubIDForUserName(email, g))
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	// Only return the user as a match if it's a member of the SCIM-managed
	// group.
	for _, ug := range u.Groups {
		if ug.GroupID == g.GroupID {
			ur, err := newUserResource(u, g)
			if err != nil {
				return nil, err
			}
			return []*UserResource{ur}, nil
		}
	}
	return nil, nil
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
		opts := &interfaces.GetGroupUsersOpts{
			Statuses:    []grpb.GroupMembershipStatus{grpb.GroupMembershipStatus_MEMBER},
			SubIDPrefix: saml.SubIDPrefixForGroup(g.URLIdentifier),
		}
		displayUsers, err := s.env.GetUserDB().GetGroupUsers(ctx, g.GroupID, opts)
		if err != nil {
			return nil, err
		}
		for _, du := range displayUsers {
			u := &tables.User{
				UserID:    du.GetUser().GetUserId().GetId(),
				FirstName: du.GetUser().GetName().GetFirst(),
				LastName:  du.GetUser().GetName().GetLast(),
				Email:     du.GetUser().GetEmail(),
				Groups: []*tables.GroupRole{
					{Group: *g, Role: uint32(du.Role)},
				},
			}
			ur, err := newUserResource(u, g)
			if err != nil {
				return nil, err
			}
			users = append(users, ur)
		}
	} else {
		fu, err := s.getFilteredUsers(ctx, g, filter)
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

	return &UserListResponseResource{
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
	ur, err := newUserResource(u, g)
	if err != nil {
		return nil, err
	}
	return ur, nil
}

func mapRole(ur *UserResource) (role.Role, error) {
	roleName := ur.Role
	if roleName == "" {
		if len(ur.Roles) > 1 {
			return 0, status.InvalidArgumentErrorf("multiple roles are not supported")
		}
		if len(ur.Roles) == 1 {
			roleName = ur.Roles[0].Value
		}
	}
	if roleName == "" {
		return role.Default, nil
	}
	return role.Parse(roleName)
}

func roleUpdateRequest(userID string, userRole role.Role, addUserToGroup bool) ([]*grpb.UpdateGroupUsersRequest_Update, error) {
	r, err := role.ToProto(userRole)
	if err != nil {
		return nil, err
	}
	update := &grpb.UpdateGroupUsersRequest_Update{
		UserId: &uidpb.UserId{Id: userID},
		Role:   r,
	}
	if addUserToGroup {
		update.MembershipAction = grpb.UpdateGroupUsersRequest_Update_ADD
	}
	return []*grpb.UpdateGroupUsersRequest_Update{update}, nil
}

func fillUserFromResource(u *tables.User, ur UserResource, g *tables.Group) error {
	userRole, err := mapRole(&ur)
	if err != nil {
		return err
	}
	u.SubID = saml.SubIDForUserName(ur.UserName, g)
	u.FirstName = ur.Name.GivenName
	u.LastName = ur.Name.FamilyName
	u.Email = ur.UserName
	u.Groups = []*tables.GroupRole{{Group: *g, Role: uint32(userRole)}}
	return nil
}

func (s *SCIMServer) createUser(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	log.CtxInfof(ctx, "SCIM create user request:\n%s", string(req))
	ur := UserResource{}
	if err := json.Unmarshal(req, &ur); err != nil {
		return nil, err
	}

	userRole, err := mapRole(&ur)
	if err != nil {
		return nil, err
	}

	// If the user exists in our system, but is not currently part of the group
	// managed by SCIM then all the SCIM query APIs will not return information
	// about this user.
	// From the perspective of the upstream system the user effectively does not
	// exist in our system. If we get a create request for such a user then
	// we need to translate it to an update request.
	updateExistingUser := false
	user, err := s.env.GetAuthDB().LookupUserFromSubID(ctx, saml.SubIDForUserName(ur.UserName, g))
	if err != nil && !status.IsNotFoundError(err) {
		return nil, err
	}
	if err == nil {
		// User already exists in our system.

		// User shouldn't already be part of the SCIM-managed group. If it is,
		// the upstream system should be sending update requests not create
		// requests.
		for _, eg := range user.Groups {
			if eg.GroupID == g.GroupID {
				return status.AlreadyExistsErrorf("user %q already exists", ur.UserName), nil
			}
		}
		updateExistingUser = true
	} else {
		// User doesn't exist. We can do a normal create.

		pk, err := tables.PrimaryKeyForTable("Users")
		if err != nil {
			return nil, err
		}
		user = &tables.User{UserID: pk}
	}

	if err := fillUserFromResource(user, ur, g); err != nil {
		return nil, err
	}

	roleUpdate, err := roleUpdateRequest(user.UserID, userRole, updateExistingUser)
	if err != nil {
		return nil, err
	}
	if updateExistingUser {
		// UpdateUser performs a permission check that allows the update only
		// if the caller is an admin of any groups of which the target user is a
		// member of. That means we need to add the user to the group first
		// before performing the update.
		if err := s.env.GetUserDB().UpdateGroupUsers(ctx, g.GroupID, roleUpdate); err != nil {
			return nil, err
		}
		if err := s.env.GetUserDB().UpdateUser(ctx, user); err != nil {
			return nil, err
		}
	} else {
		if err := s.env.GetUserDB().InsertUser(ctx, user); err != nil {
			return nil, err
		}
		if err := s.env.GetUserDB().UpdateGroupUsers(ctx, g.GroupID, roleUpdate); err != nil {
			return nil, err
		}
	}
	return newUserResource(user, g)
}

// Azure AD incorrectly sends "active" field as a string instead of a native
// boolean...
func getBooleanValue(v any) (bool, error) {
	if v, ok := v.(bool); ok {
		return v, nil
	}
	if v, ok := v.(string); ok {
		return strings.EqualFold(v, "true"), nil
	}
	return false, status.InvalidArgumentErrorf("boolean field has unexpected value %v of type %T", v, v)
}

func (s *SCIMServer) patchUser(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	log.CtxInfof(ctx, "Patch user request:\n%s", string(req))
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
	var newRole *string

	handleAttr := func(name string, value any) error {
		switch name {
		case ActiveAttribute:
			b, err := getBooleanValue(value)
			if err != nil {
				return err
			}
			deleteUser = !b
		case GivenNameAttribute:
			v, ok := value.(string)
			if !ok {
				return status.InvalidArgumentErrorf("expected string attribute for given name but got %T", value)
			}
			u.FirstName = v
		case FamilyNameAttribute:
			v, ok := value.(string)
			if !ok {
				return status.InvalidArgumentErrorf("expected string attribute for family name but got %T", value)
			}
			u.LastName = v
		case RoleAttribute:
			v, ok := value.(string)
			if !ok {
				return status.InvalidArgumentErrorf("expected string attribute for role but got %T", value)
			}
			newRole = &v
		case UserNameAttribute:
			v, ok := value.(string)
			if !ok {
				return status.InvalidArgumentErrorf("expected string attribute for username but got %T", value)
			}
			u.Email = v
			u.SubID = saml.SubIDForUserName(v, g)
		default:
			return status.InvalidArgumentErrorf("unsupported attribute %q", name)
		}
		return nil
	}

	for _, op := range pr.Operations {
		if !strings.EqualFold(op.Op, "replace") && !strings.EqualFold(op.Op, "add") {
			return nil, status.InvalidArgumentErrorf("unsupported operation %q", op.Op)
		}

		if op.Path == "" {
			// If path is not set, then the value is a map of the properties to be
			// modified.
			m, ok := op.Value.(map[string]any)
			if !ok {
				return nil, status.InvalidArgumentErrorf("path was empty, but value was not a map but %T", op.Value)
			}
			for k, v := range m {
				err := handleAttr(k, v)
				if err != nil {
					return nil, err
				}
			}
		} else {
			err := handleAttr(op.Path, op.Value)
			if err != nil {
				return nil, err
			}
		}
	}

	ur, err := newUserResource(u, g)
	if err != nil {
		return nil, err
	}
	if deleteUser {
		err = s.env.GetUserDB().DeleteUser(ctx, id)
		if err != nil {
			return nil, err
		}
		ur.Active = false
	} else {
		if newRole != nil {
			ur.Role = *newRole
			ur.Roles = []RoleResource{{Primary: true, Value: *newRole}}
			userRole, err := mapRole(ur)
			if err != nil {
				return nil, err
			}
			roleUpdate, err := roleUpdateRequest(id, userRole, false /*=addUserToGroup*/)
			if err != nil {
				return nil, err
			}
			if err := s.env.GetUserDB().UpdateGroupUsers(ctx, g.GroupID, roleUpdate); err != nil {
				return nil, err
			}
		}
		if err := s.env.GetUserDB().UpdateUser(ctx, u); err != nil {
			return nil, err
		}
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
	u.Email = ur.UserName
	u.SubID = saml.SubIDForUserName(ur.UserName, g)
	updatedUser, err := newUserResource(u, g)
	if err != nil {
		return nil, err
	}
	if !ur.Active {
		err = s.env.GetUserDB().DeleteUser(ctx, id)
		if err != nil {
			return nil, err
		}
		updatedUser.Active = false
	} else {
		if err := s.env.GetUserDB().UpdateUser(ctx, u); err != nil {
			return nil, err
		}
		userRole, err := mapRole(&ur)
		if err != nil {
			return nil, err
		}
		roleUpdate, err := roleUpdateRequest(id, userRole, false /*=addUserToGroup*/)
		if err != nil {
			return nil, err
		}
		if err := s.env.GetUserDB().UpdateGroupUsers(ctx, g.GroupID, roleUpdate); err != nil {
			return nil, err
		}
		updatedUser.Role = ur.Role
	}
	return updatedUser, nil
}

func (s *SCIMServer) deleteUser(ctx context.Context, r *http.Request, g *tables.Group) (interface{}, error) {
	id := path.Base(r.URL.Path)
	if err := s.env.GetUserDB().DeleteUser(ctx, id); err != nil {
		return nil, err
	}
	return nil, nil
}
