package userdb

import (
	"context"
	"database/sql"
	"flag"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
)

const (
	// Default label shown on API keys that do not have a custom label.
	// Changing this will change the default label text shown on new API
	// keys if the user leaves the label field blank.
	defaultAPIKeyLabel = "Default"
)

var (
	addUserToDomainGroup = flag.Bool("app.add_user_to_domain_group", false, "Cloud-Only")
	createGroupPerUser   = flag.Bool("app.create_group_per_user", false, "Cloud-Only")
	noDefaultUserGroup   = flag.Bool("app.no_default_user_group", false, "Cloud-Only")

	orgName   = flag.String("org.name", "Organization", "The name of your organization, which is displayed on your organization's build history.")
	orgDomain = flag.String("org.domain", "", "Your organization's email domain. If this is set, only users with email addresses in this domain will be able to register for a BuildBuddy account.")

	// Don't change this ever. It's the default group that users
	// are added to when the server is running on-prem and group
	// management is not really an issue. If you change it, you'll
	// likely cause everyone to be added to a new group and break
	// existing invocation history if not more.
	DefaultGroupID = "GR0000000000000000000"
	blockList      = []string{
		"aim.com", "alice.it", "aliceadsl.fr", "aol.com", "arcor.de", "att.net", "bellsouth.net", "bigpond.com", "bigpond.net.au", "bluewin.ch", "blueyonder.co.uk", "bol.com.br", "centurytel.net", "charter.net", "chello.nl", "club-internet.fr", "comcast.net", "cox.net", "earthlink.net", "facebook.com", "free.fr", "freenet.de", "frontiernet.net", "gmail.com", "gmx.de", "gmx.net", "googlemail.com", "hetnet.nl", "home.nl", "hotmail.co.uk", "hotmail.com", "hotmail.de", "hotmail.es", "hotmail.fr", "hotmail.it", "ig.com.br", "juno.com", "laposte.net", "libero.it", "live.ca", "live.co.uk", "live.com", "live.com.au", "live.fr", "live.it", "live.nl", "mac.com", "mail.com", "mail.ru", "me.com", "msn.com", "neuf.fr", "ntlworld.com", "optonline.net", "optusnet.com.au", "orange.fr", "outlook.com", "planet.nl", "qq.com", "rambler.ru", "rediffmail.com", "rocketmail.com", "sbcglobal.net", "sfr.fr", "shaw.ca", "sky.com", "skynet.be", "sympatico.ca", "t-online.de", "telenet.be", "terra.com.br", "tin.it", "tiscali.co.uk", "tiscali.it", "uol.com.br", "verizon.net", "virgilio.it", "voila.fr", "wanadoo.fr", "web.de", "windstream.net", "yahoo.ca", "yahoo.co.id", "yahoo.co.in", "yahoo.co.jp", "yahoo.co.uk", "yahoo.com", "yahoo.com.ar", "yahoo.com.au", "yahoo.com.br", "yahoo.com.mx", "yahoo.com.sg", "yahoo.de", "yahoo.es", "yahoo.fr", "yahoo.in", "yahoo.it", "yandex.ru", "ymail.com", "zonnet.nl"}
	// Group URL identifiers can only contain a-z, 0-9, and hyphen
	groupUrlIdentifierPattern = regexp.MustCompile(`^[a-z0-9\-]+$`)

	// Default capabilities for group-owned API keys.
	defaultAPIKeyCapabilities = []akpb.ApiKey_Capability{
		akpb.ApiKey_CACHE_WRITE_CAPABILITY,
	}
)

func singleUserGroup(u *tables.User) (*tables.Group, error) {
	name := "My Organization"
	if u.Email != "" {
		name = u.Email
	}
	if u.FirstName != "" || u.LastName != "" {
		name = strings.TrimSpace(strings.Join([]string{u.FirstName, u.LastName}, " "))
	}

	return &tables.Group{
		GroupID: strings.Replace(u.UserID, "US", "GR", 1),
		UserID:  u.UserID,
		Name:    name,
	}, nil
}

type UserDB struct {
	env environment.Env
	h   interfaces.DBHandle
}

func NewUserDB(env environment.Env, h interfaces.DBHandle) (*UserDB, error) {
	db := &UserDB{
		env: env,
		h:   h,
	}
	if !*noDefaultUserGroup {
		if err := db.CreateDefaultGroup(context.Background()); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (d *UserDB) GetGroupByID(ctx context.Context, groupID string) (*tables.Group, error) {
	return d.getGroupByID(ctx, d.h, groupID)
}

func (d *UserDB) getGroupByID(ctx context.Context, h interfaces.DB, groupID string) (*tables.Group, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be empty.")
	}
	rq := h.NewQuery(ctx, "userdb_get_group_by_id").Raw(
		`SELECT * FROM "Groups" AS g WHERE g.group_id = ?`, groupID)
	group := &tables.Group{}
	if err := rq.Take(group); err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("The requested organization was not found.")
		}
		return nil, err
	}
	return group, nil
}

func (d *UserDB) GetGroupByURLIdentifier(ctx context.Context, urlIdentifier string) (*tables.Group, error) {
	group, err := d.getGroupByURLIdentifier(ctx, d.h, urlIdentifier)
	if err != nil {
		return nil, err
	}
	return group, err
}

func (d *UserDB) getGroupByURLIdentifier(ctx context.Context, h interfaces.DB, urlIdentifier string) (*tables.Group, error) {
	if urlIdentifier == "" {
		return nil, status.InvalidArgumentError("URL identifier cannot be empty.")
	}
	rq := h.NewQuery(ctx, "userdb_get_group_by_identifier").Raw(
		`SELECT * FROM "Groups" AS g WHERE g.url_identifier = ?`, urlIdentifier)
	group := &tables.Group{}
	if err := rq.Take(group); err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("The requested organization was not found.")
		}
		return nil, err
	}
	return group, nil
}

func (d *UserDB) DeleteUserGitHubToken(ctx context.Context) error {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	if u.GetUserID() == "" {
		return status.FailedPreconditionError("user ID must not be empty")
	}
	return d.h.NewQuery(ctx, "userdb_delete_user_github_token").Raw(
		`UPDATE "Users" SET github_token = '' WHERE user_id = ?`,
		u.GetUserID(),
	).Exec().Error
}

func (d *UserDB) authorizeGroupAdminRole(ctx context.Context, groupID string) error {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	return authutil.AuthorizeOrgAdmin(u, groupID)
}

func isInOwnedDomainBlocklist(email string) bool {
	for _, item := range blockList {
		if item == email {
			return true
		}
	}
	return false
}

func (d *UserDB) getDomainOwnerGroup(ctx context.Context, h interfaces.DB, domain string) (*tables.Group, error) {
	tg := &tables.Group{}

	err := h.NewQuery(ctx, "userdb_get_domain_owner_group").Raw(
		`SELECT * FROM "Groups" as g WHERE g.owned_domain = ?`,
		domain,
	).Take(tg)

	if db.IsRecordNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return tg, nil
}

func getUserGroup(ctx context.Context, h interfaces.DB, userID string, groupID string) (*tables.UserGroup, error) {
	userGroup := &tables.UserGroup{}
	rq := h.NewQuery(ctx, "userdb_get_user_group").Raw(
		`SELECT * FROM "UserGroups" AS ug WHERE ug.user_user_id = ? AND ug.group_group_id = ?`, userID, groupID)
	if err := rq.Take(userGroup); err != nil {
		if db.IsRecordNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return userGroup, nil
}

// validateURLIdentifier verifies that the proposed urlIdentifier for groupID
// is not a reserved identifier and is not already used by a different group.
// Returns true if the identifier may be used.
func (d *UserDB) validateURLIdentifier(ctx context.Context, groupID string, urlIdentifier string) (bool, error) {
	if urlIdentifier == "" {
		return true, nil
	}

	if subdomain.Enabled() {
		for _, sd := range subdomain.DefaultSubdomains() {
			if urlIdentifier == sd {
				return false, nil
			}
		}
	}

	existingGroup, err := d.GetGroupByURLIdentifier(ctx, urlIdentifier)
	if err != nil {
		if status.IsNotFoundError(err) {
			return true, nil
		}
		return false, err
	}
	if existingGroup.GroupID != groupID {
		return false, nil
	}
	return true, nil
}

func (d *UserDB) CreateGroup(ctx context.Context, g *tables.Group) (string, error) {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return "", err
	}
	if g.URLIdentifier != "" {
		valid, err := d.validateURLIdentifier(ctx, "", g.URLIdentifier)
		if err != nil {
			return "", err
		}
		if !valid {
			return "", status.InvalidArgumentError("URL is already in use")
		}
	}

	currentGroup, err := d.env.GetUserDB().GetGroupByID(ctx, u.GetGroupID())
	// If the user is in at least one group, and we can't look up the selected group - return an error.
	if len(u.GetGroupMemberships()) > 0 && err != nil {
		return "", err
	}

	err = authutil.AuthorizeOrgAdmin(u, u.GetGroupID())

	// We can continue if one of the following is true:
	// 1) doesn't have an existing group
	// 2) an user is an admin
	// 3) developers are allowed to create organizations in their existing organization
	// Otherwise we return that the user doesn't have permissions to create an organization.
	if err != nil && currentGroup != nil && !currentGroup.DeveloperOrgCreationEnabled {
		return "", status.UnauthenticatedErrorf("You don't have permission to create a group")
	}

	groupID := ""
	err = d.h.Transaction(ctx, func(tx interfaces.DB) error {
		gid, err := d.createGroup(ctx, tx, u.GetUserID(), g)
		groupID = gid
		return err
	})
	if err != nil {
		return "", err
	}
	return groupID, nil
}

func (d *UserDB) createGroup(ctx context.Context, tx interfaces.DB, userID string, g *tables.Group) (string, error) {
	if g.GroupID != "" {
		return "", status.InvalidArgumentErrorf("cannot create a Group using an existing group ID")
	}
	newGroup := *g // copy
	groupID, err := tables.PrimaryKeyForTable("Groups")
	if err != nil {
		return "", err
	}
	newGroup.GroupID = groupID

	if err := tx.NewQuery(ctx, "userdb_create_group").Create(&newGroup); err != nil {
		return "", err
	}
	// Initialize the group with a group-owned key.
	_, err = d.env.GetAuthDB().CreateAPIKeyWithoutAuthCheck(ctx, tx, groupID, defaultAPIKeyLabel, defaultAPIKeyCapabilities, false /*visibleToDevelopers*/)
	if err != nil {
		return "", err
	}
	if err = d.addUserToGroup(ctx, tx, userID, groupID); err != nil {
		return "", err
	}
	return groupID, nil
}

func (d *UserDB) InsertOrUpdateGroup(ctx context.Context, g *tables.Group) (string, error) {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return "", err
	}
	if isInOwnedDomainBlocklist(g.OwnedDomain) {
		return "", status.InvalidArgumentError("This domain is not allowed to be owned by any group.")
	}
	if match := groupUrlIdentifierPattern.MatchString(g.URLIdentifier); !match {
		return "", status.InvalidArgumentError("Invalid organization URL.")
	}

	valid, err := d.validateURLIdentifier(ctx, g.GroupID, g.URLIdentifier)
	if err != nil {
		return "", err
	}
	if !valid {
		return "", status.InvalidArgumentError("URL is already in use")
	}

	groupID := ""
	err = d.h.Transaction(ctx, func(tx interfaces.DB) error {
		if g.OwnedDomain != "" {
			existingDomainOwnerGroup, err := d.getDomainOwnerGroup(ctx, tx, g.OwnedDomain)
			if err != nil {
				return err
			}
			if existingDomainOwnerGroup != nil && existingDomainOwnerGroup.GroupID != g.GroupID {
				return status.InvalidArgumentError("There is already a group associated with this domain.")
			}
		}
		if g.GroupID == "" {
			groupID, err = d.createGroup(ctx, tx, u.GetUserID(), g)
			return err
		}

		groupID = g.GroupID
		if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
			return err
		}

		res := tx.NewQuery(ctx, "userdb_update_group").Raw(`
			UPDATE "Groups" SET
				name = ?,
				url_identifier = ?,
				owned_domain = ?,
				sharing_enabled = ?,
				user_owned_keys_enabled = ?,
				bot_suggestions_enabled = ?,
				developer_org_creation_enabled = ?,
				use_group_owned_executors = ?,
				cache_encryption_enabled = ?,
				suggestion_preference = ?,
				restrict_clean_workflow_runs_to_admins = ?,
				enforce_ip_rules = ?
			WHERE group_id = ?`,
			g.Name,
			g.URLIdentifier,
			g.OwnedDomain,
			g.SharingEnabled,
			g.UserOwnedKeysEnabled,
			g.BotSuggestionsEnabled,
			g.DeveloperOrgCreationEnabled,
			g.UseGroupOwnedExecutors,
			g.CacheEncryptionEnabled,
			g.SuggestionPreference,
			g.RestrictCleanWorkflowRunsToAdmins,
			g.EnforceIPRules,
			g.GroupID).Exec()
		if res.Error != nil {
			return res.Error
		}
		return nil
	})
	return groupID, err
}

func (d *UserDB) DeleteGroupGitHubToken(ctx context.Context, groupID string) error {
	q, args := query_builder.
		NewQuery(`UPDATE "Groups" SET github_token = ''`).
		AddWhereClause("group_id = ?", groupID).
		Build()
	return d.h.NewQuery(ctx, "userdb_delete_group_github_token").Raw(q, args...).Exec().Error
}

func (d *UserDB) addUserToGroup(ctx context.Context, tx interfaces.DB, userID, groupID string) (retErr error) {
	// Count the number of users in the group.
	// If there are no existing users, then user should join with admin role,
	// otherwise they should join with default role.
	row := &struct{ Count int64 }{}
	err := tx.NewQuery(ctx, "userdb_check_group_size").Raw(`
		SELECT COUNT(*) AS count FROM "UserGroups"
		WHERE group_group_id = ? AND membership_status = ?
	`, groupID, grpb.GroupMembershipStatus_MEMBER).Take(row)
	if err != nil {
		return err
	}
	r := role.Default
	if row.Count == 0 {
		r = role.Admin
	}

	existing, err := getUserGroup(ctx, tx, userID, groupID)
	if err != nil && !db.IsRecordNotFound(err) {
		return err
	}
	if existing != nil {
		if existing.MembershipStatus == int32(grpb.GroupMembershipStatus_REQUESTED) {
			return tx.NewQuery(ctx, "userdb_add_requested_user_to_group").Raw(`
				UPDATE "UserGroups"
				SET membership_status = ?, role = ?
				WHERE user_user_id = ?
				AND group_group_id = ?
				`,
				grpb.GroupMembershipStatus_MEMBER,
				r,
				userID,
				groupID,
			).Exec().Error
		}
		return status.AlreadyExistsError("You're already in this organization.")
	}
	return tx.NewQuery(ctx, "userdb_add_user_to_group").Raw(
		`INSERT INTO "UserGroups" (user_user_id, group_group_id, membership_status, role) VALUES(?, ?, ?, ?)`,
		userID, groupID, int32(grpb.GroupMembershipStatus_MEMBER), r,
	).Exec().Error
}

func (d *UserDB) RequestToJoinGroup(ctx context.Context, groupID string) (grpb.GroupMembershipStatus, error) {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return 0, err
	}
	userID := u.GetUserID()
	if userID == "" {
		return 0, status.InvalidArgumentError("User ID is required.")
	}
	if groupID == "" {
		return 0, status.InvalidArgumentError("Group ID is required.")
	}
	var membershipStatus grpb.GroupMembershipStatus
	err = d.h.Transaction(ctx, func(tx interfaces.DB) error {
		tu, err := d.getUser(ctx, tx, u.GetUserID())
		if err != nil {
			return err
		}
		group, err := d.getGroupByID(ctx, tx, groupID)
		if err != nil {
			return err
		}
		// If the org has an owned domain that matches the user's email,
		// the user can join directly as a member.
		membershipStatus = grpb.GroupMembershipStatus_REQUESTED
		if !u.IsSAML() && group.OwnedDomain != "" && group.OwnedDomain == getEmailDomain(tu.Email) {
			membershipStatus = grpb.GroupMembershipStatus_MEMBER
			return d.addUserToGroup(ctx, tx, userID, groupID)
		}

		// Check if there's an existing request and return AlreadyExists if so.
		existing, err := getUserGroup(ctx, tx, userID, groupID)
		if err != nil {
			return err
		}
		if existing != nil {
			if existing.MembershipStatus == int32(grpb.GroupMembershipStatus_REQUESTED) {
				return status.AlreadyExistsError("You've already requested to join this organization.")
			}
			return status.AlreadyExistsError("You're already in this organization.")
		}
		return tx.NewQuery(ctx, "userdb_create_join_group_request").Create(&tables.UserGroup{
			UserUserID:       userID,
			GroupGroupID:     groupID,
			Role:             uint32(role.Default),
			MembershipStatus: int32(membershipStatus),
		})
	})
	if err != nil {
		return 0, err
	}
	return membershipStatus, nil
}

func (d *UserDB) GetGroupUsers(ctx context.Context, groupID string, statuses []grpb.GroupMembershipStatus) ([]*grpb.GetGroupUsersResponse_GroupUser, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be empty.")
	}
	if len(statuses) == 0 {
		return nil, status.InvalidArgumentError("A valid status or statuses are required")
	}
	if err := perms.AuthorizeGroupAccess(ctx, d.env, groupID); err != nil {
		return nil, err
	}

	users := make([]*grpb.GetGroupUsersResponse_GroupUser, 0)

	q := query_builder.NewQuery(`
			SELECT u.user_id, u.email, u.first_name, u.last_name, ug.membership_status, ug.role
			FROM "Users" AS u JOIN "UserGroups" AS ug ON u.user_id = ug.user_user_id`)
	q = q.AddWhereClause(`ug.group_group_id = ?`, groupID)

	o := query_builder.OrClauses{}
	for _, s := range statuses {
		if s == grpb.GroupMembershipStatus_UNKNOWN_MEMBERSHIP_STATUS {
			return nil, status.InvalidArgumentError("Invalid status filter")
		}
		o.AddOr(`ug.membership_status = ?`, int32(s))
	}
	orQuery, orArgs := o.Build()
	q = q.AddWhereClause("("+orQuery+")", orArgs...)

	q.SetOrderBy(`u.email`, true /*=ascending*/)

	qString, qArgs := q.Build()
	rq := d.h.NewQuery(ctx, "userdb_get_group_users").Raw(qString, qArgs...)
	err := rq.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
		groupUser := &grpb.GetGroupUsersResponse_GroupUser{}
		user := &tables.User{}
		var groupRole uint32
		err := row.Scan(
			&user.UserID, &user.Email, &user.FirstName, &user.LastName,
			&groupUser.GroupMembershipStatus, &groupRole,
		)
		if err != nil {
			return err
		}
		groupUser.User = user.ToProto()
		r, err := role.ToProto(role.Role(groupRole))
		if err != nil {
			return err
		}
		groupUser.Role = r
		users = append(users, groupUser)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return users, nil
}

func (d *UserDB) removeUserFromGroup(ctx context.Context, tx interfaces.DB, userID string, groupID string) error {
	ug, err := getUserGroup(ctx, tx, userID, groupID)
	if err != nil {
		return err
	}
	if ug == nil {
		return nil
	}
	if err := tx.NewQuery(ctx, "userdb_remove_user_from_group").Raw(`
						DELETE FROM "UserGroups"
						WHERE user_user_id = ? AND group_group_id = ?`,
		userID,
		groupID).Exec().Error; err != nil {
		return err
	}
	if err := tx.NewQuery(ctx, "userdb_delete_user_api_keys").Raw(`
						DELETE FROM "APIKeys"
						WHERE user_id = ? AND group_id = ?`,
		userID,
		groupID).Exec().Error; err != nil {
		return err
	}
	return nil
}

func (d *UserDB) updateUserRole(ctx context.Context, tx interfaces.DB, userID string, groupID string, newRole role.Role) error {
	ug, err := getUserGroup(ctx, tx, userID, groupID)
	if err != nil {
		return err
	}
	if ug == nil {
		return nil
	}
	if role.Role(ug.Role) == newRole {
		return nil
	}
	maxCapabilitiesForNewRole, err := role.ToCapabilities(newRole)
	if err != nil {
		return err
	}
	err = tx.NewQuery(ctx, "userdb_update_user_role").Raw(`
		UPDATE "UserGroups"
		SET role = ?
		WHERE user_user_id = ? AND group_group_id = ?
	`, newRole, userID, groupID,
	).Exec().Error
	if err != nil {
		return err
	}
	// Update capabilities to reflect the new role.
	//
	// In this query, there is an edge case to handle: we need to augment the
	// existing capabilities with an explicit CAS_WRITE capability if it was
	// previously implicitly allowed via CACHE_WRITE.
	//
	// More concretely, the expression `((capabilities & 1) << 2)` evaluates to
	// CAS_WRITE if the user currently has CACHE_WRITE, otherwise 0. We then OR
	// this with the user's current capabilities.
	err = tx.NewQuery(ctx, "userdb_update_user_role_api_key_capabilities").Raw(`
		UPDATE "APIKeys"
		SET capabilities = (capabilities | ((capabilities & 1) << 2)) & ?
		WHERE user_id = ? AND group_id = ?
	`, capabilities.ToInt(maxCapabilitiesForNewRole), userID, groupID).Exec().Error
	if err != nil {
		return err
	}
	return nil
}

func (d *UserDB) UpdateGroupUsers(ctx context.Context, groupID string, updates []*grpb.UpdateGroupUsersRequest_Update) error {
	if err := d.authorizeGroupAdminRole(ctx, groupID); err != nil {
		return err
	}
	for _, u := range updates {
		if u.GetUserId().GetId() == "" {
			return status.InvalidArgumentError("update contains an empty user ID")
		}
	}
	return d.h.Transaction(ctx, func(tx interfaces.DB) error {
		for _, update := range updates {
			switch update.GetMembershipAction() {
			case grpb.UpdateGroupUsersRequest_Update_REMOVE:
				if err := d.removeUserFromGroup(ctx, tx, update.GetUserId().GetId(), groupID); err != nil {
					return err
				}
			case grpb.UpdateGroupUsersRequest_Update_ADD:
				if err := d.addUserToGroup(ctx, tx, update.GetUserId().GetId(), groupID); err != nil {
					return err
				}
			case grpb.UpdateGroupUsersRequest_Update_UNKNOWN_MEMBERSHIP_ACTION:
			default:
				return status.InvalidArgumentError("Invalid membership action")
			}

			if update.Role != grpb.Group_UNKNOWN_ROLE {
				r, err := role.FromProto(update.GetRole())
				if err != nil {
					return err
				}
				if err := d.updateUserRole(ctx, tx, update.GetUserId().GetId(), groupID, r); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (d *UserDB) CreateDefaultGroup(ctx context.Context) error {
	return d.h.Transaction(ctx, func(tx interfaces.DB) error {
		var existing tables.Group
		if err := tx.GORM(ctx, "userdb_check_existing_group").Where("group_id = ?", DefaultGroupID).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				gc := d.getDefaultGroupConfig()
				if err := tx.NewQuery(ctx, "userdb_create_default_group").Create(gc); err != nil {
					return err
				}
				if _, err := d.env.GetAuthDB().CreateAPIKeyWithoutAuthCheck(ctx, tx, DefaultGroupID, defaultAPIKeyLabel, defaultAPIKeyCapabilities, false /*visibleToDevelopers*/); err != nil {
					return err
				}
				return nil
			}
			return err
		}

		return tx.GORM(ctx, "userdb_update_existing_group").Model(&tables.Group{}).Where("group_id = ?", DefaultGroupID).Updates(d.getDefaultGroupConfig()).Error
	})
}

func (d *UserDB) getDefaultGroupConfig() *tables.Group {
	return &tables.Group{
		GroupID:     DefaultGroupID,
		Name:        *orgName,
		OwnedDomain: *orgDomain,
	}
}

func (d *UserDB) createUser(ctx context.Context, tx interfaces.DB, u *tables.User) error {
	if u.UserID == "" {
		return status.FailedPreconditionError("UserID is required")
	}
	if u.SubID == "" {
		return status.FailedPreconditionError("SubID is required")
	}
	emailDomain := ""
	if u.Email != "" {
		emailParts := strings.Split(u.Email, "@")
		if len(emailParts) != 2 {
			return status.FailedPreconditionErrorf("Invalid email address: %s", u.Email)
		}
		emailDomain = emailParts[1]
	}

	groupIDs := make([]string, 0)
	for _, group := range u.Groups {
		hydratedGroup, err := d.getGroupByURLIdentifier(ctx, tx, group.Group.URLIdentifier)
		if err != nil {
			return err
		}
		groupIDs = append(groupIDs, hydratedGroup.GroupID)
	}

	// If the user signed up using an authenticator associated with a group (i.e. SAML or OIDC SSO),
	// don't add it to a group based on domain.
	if len(u.Groups) == 0 && *addUserToDomainGroup && emailDomain != "" {
		dg, err := d.getDomainOwnerGroup(ctx, tx, emailDomain)
		if err != nil {
			log.Errorf("error in createUser: %s", err)
			return err
		}
		if dg != nil {
			groupIDs = append(groupIDs, dg.GroupID)
		}
	}

	// If the user isn't associated with an org, create a personal group.
	if len(groupIDs) == 0 && *createGroupPerUser {
		sug, err := singleUserGroup(u)
		if err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "userdb_create_user_group").Create(&sug); err != nil {
			return err
		}
		// For now, user-owned groups are assigned an org-level API key, and
		// users have to explicitly enable user-owned keys.
		if _, err := d.env.GetAuthDB().CreateAPIKeyWithoutAuthCheck(ctx, tx, sug.GroupID, defaultAPIKeyLabel, defaultAPIKeyCapabilities, false /*visibleToDevelopers*/); err != nil {
			return err
		}
		groupIDs = append(groupIDs, sug.GroupID)
	}

	if !*noDefaultUserGroup {
		g := d.getDefaultGroupConfig()
		if g.OwnedDomain != "" && g.OwnedDomain != emailDomain {
			return status.FailedPreconditionErrorf("Failed to create user, email address: %s does not belong to domain: %s", u.Email, g.OwnedDomain)
		}
		groupIDs = append(groupIDs, DefaultGroupID)
	}

	err := tx.NewQuery(ctx, "userdb_create_user").Create(u)
	if err != nil {
		return err
	}

	for _, groupID := range groupIDs {
		err := tx.NewQuery(ctx, "userdb_new_user_create_groups").Raw(`
			INSERT INTO "UserGroups" (user_user_id, group_group_id, membership_status, role)
			VALUES (?, ?, ?, ?)
			`, u.UserID, groupID, int32(grpb.GroupMembershipStatus_MEMBER), uint32(role.Default),
		).Exec().Error
		if err != nil {
			return err
		}
		// Promote from default role to admin if the user is the only one in the
		// group after joining.
		preExistingUsers := &struct{ Count int64 }{}
		err = tx.GORM(ctx, "userdb_new_user_check_group_size").Raw(`
			SELECT COUNT(*) AS count
			FROM "UserGroups"
			WHERE group_group_id = ? AND user_user_id != ?
			`, groupID, u.UserID,
		).Scan(preExistingUsers).Error
		if err != nil {
			return err
		}
		if preExistingUsers.Count > 0 {
			continue
		}
		err = tx.NewQuery(ctx, "userdb_new_user_promote_admin").Raw(`
			UPDATE "UserGroups"
			SET role = ?
			WHERE group_group_id = ?
			`, uint32(role.Admin), groupID,
		).Exec().Error
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *UserDB) InsertUser(ctx context.Context, u *tables.User) error {
	return d.h.Transaction(ctx, func(tx interfaces.DB) error {
		var existing tables.User
		if err := tx.GORM(ctx, "userdb_check_existing_user").Where("sub_id = ?", u.SubID).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				return d.createUser(ctx, tx, u)
			}
			return err
		}
		return status.FailedPreconditionError("User already exists!")
	})
}

func (d *UserDB) GetUserByID(ctx context.Context, id string) (*tables.User, error) {
	var user *tables.User
	err := d.h.Transaction(ctx, func(tx interfaces.DB) error {
		u, err := d.getUser(ctx, tx, id)
		if err != nil {
			return err
		}
		user = u
		return err
	})
	if err != nil {
		return nil, err
	}

	authUser, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range user.Groups {
		if err := authutil.AuthorizeOrgAdmin(authUser, g.Group.GroupID); err == nil {
			return user, nil
		}
	}
	return nil, status.NotFoundError("user not found")
}

func (d *UserDB) GetUserByIDWithoutAuthCheck(ctx context.Context, id string) (*tables.User, error) {
	var user *tables.User
	err := d.h.Transaction(ctx, func(tx interfaces.DB) error {
		u, err := d.getUser(ctx, tx, id)
		if err != nil {
			return err
		}
		user = u
		return err
	})
	return user, err
}

func (d *UserDB) GetUserByEmail(ctx context.Context, email string) (*tables.User, error) {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	// TODO(zoey): switch this to JOIN
	rq := d.h.NewQuery(ctx, "userdb_get_user_by_email").Raw(`
		SELECT * 
		FROM "Users" u 
		JOIN "UserGroups" ug on u.user_id = ug.user_user_id
		WHERE u.email = ? AND ug.group_group_id = ?
	`, email, u.GetGroupID())
	users, err := db.ScanAll(rq, &tables.User{})
	if err != nil {
		return nil, err
	}
	switch len(users) {
	case 0:
		return nil, status.NotFoundErrorf("no users found with email %q", email)
	case 1:
		user := users[0]
		if err := fillUserGroups(ctx, d.h, user); err != nil {
			return nil, err
		}
		return user, nil
	default:
		return nil, status.FailedPreconditionErrorf("multiple users found for email %q", email)
	}
}

func (d *UserDB) GetUser(ctx context.Context) (*tables.User, error) {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.IsImpersonating() {
		return d.GetImpersonatedUser(ctx)
	}
	var user *tables.User
	err = d.h.Transaction(ctx, func(tx interfaces.DB) error {
		user, err = d.getUser(ctx, tx, u.GetUserID())
		return err
	})
	return user, err
}

func fillUserGroups(ctx context.Context, h interfaces.DB, user *tables.User) error {
	rq := h.NewQuery(ctx, "userdb_get_user_groups").Raw(`
			SELECT g.*, ug.role
			FROM "Groups" as g
			JOIN "UserGroups" as ug
			ON g.group_id = ug.group_group_id
			WHERE ug.user_user_id = ? AND ug.membership_status = ?
		`,
		user.UserID,
		int32(grpb.GroupMembershipStatus_MEMBER),
	)
	gs, err := db.ScanAll(rq, &tables.GroupRole{})
	if err != nil {
		return err
	}
	user.Groups = gs
	return nil
}

func (d *UserDB) getUser(ctx context.Context, tx interfaces.DB, userID string) (*tables.User, error) {
	// TODO(zoey): switch this to JOIN
	user := &tables.User{}
	err := tx.NewQuery(ctx, "userdb_get_user").Raw(
		`SELECT * FROM "Users" WHERE user_id = ?`,
		userID,
	).Take(user)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("user not found")
		}
		return nil, err
	}
	if err := fillUserGroups(ctx, tx, user); err != nil {
		return nil, err
	}
	return user, nil
}

func (d *UserDB) GetImpersonatedUser(ctx context.Context) (*tables.User, error) {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if !u.IsImpersonating() {
		return nil, status.PermissionDeniedError("Authenticated user does not have permissions to impersonate a user.")
	}
	user := &tables.User{}
	err = d.h.NewQuery(ctx, "userdb_impersonation_get_user").Raw(
		`SELECT * FROM "Users" WHERE user_id = ?`,
		u.GetUserID(),
	).Take(user)
	if err != nil {
		return nil, err
	}
	gr := &tables.GroupRole{}
	err = d.h.NewQuery(ctx, "userdb_impersonation_get_group").Raw(`
			SELECT *
			FROM "Groups"
			WHERE group_id = ?
		`,
		u.GetGroupID(),
	).Take(gr)
	if err != nil {
		return nil, err
	}
	// Grant admin role within the impersonated group.
	gr.Role = uint32(role.Admin)
	user.Groups = []*tables.GroupRole{gr}
	return user, nil
}

func (d *UserDB) DeleteUser(ctx context.Context, userID string) error {
	// Permission check.
	_, err := d.GetUserByID(ctx, userID)
	if err != nil {
		return err
	}
	return d.h.Transaction(ctx, func(tx interfaces.DB) error {
		rq := tx.NewQuery(ctx, "userdb_delete_user_memberships").Raw(`
			DELETE FROM "UserGroups" WHERE user_user_id = ?`, userID)
		if err := rq.Exec().Error; err != nil {
			return err
		}
		rq = tx.NewQuery(ctx, "userdb_delete_user_all_keys").Raw(`
			DELETE FROM "APIKeys" WHERE user_id = ?`, userID)
		if err := rq.Exec().Error; err != nil {
			return err
		}
		rq = tx.NewQuery(ctx, "userdb_delete_user").Raw(`
			DELETE FROM "Users" WHERE user_id = ?`, userID)
		if err := rq.Exec().Error; err != nil {
			return err
		}
		return nil
	})
}

func (d *UserDB) UpdateUser(ctx context.Context, u *tables.User) error {
	// Permission check.
	_, err := d.GetUserByID(ctx, u.UserID)
	if err != nil {
		return err
	}
	return d.h.NewQuery(ctx, "userdb_update_user").Update(u)
}

func (d *UserDB) FillCounts(ctx context.Context, stat *telpb.TelemetryStat) error {
	rq := d.h.NewQuery(ctx, "userdb_get_user_count").Raw(`
		SELECT 
			COUNT(DISTINCT user_id) as registered_user_count
		FROM "Users" as u
		WHERE 
			u.created_at_usec >= ? AND
			u.created_at_usec < ?`,
		time.Now().Truncate(24*time.Hour).Add(-24*time.Hour).UnixMicro(),
		time.Now().Truncate(24*time.Hour).UnixMicro())

	if err := rq.Take(stat); err != nil {
		return err
	}
	return nil
}

func (d *UserDB) GetOrCreatePublicKey(ctx context.Context, groupID string) (string, error) {
	tg, err := d.GetGroupByID(ctx, groupID)
	if err != nil {
		return "", err
	}
	if tg.PublicKey != "" {
		return tg.PublicKey, nil
	}

	// Still here? Group may not have a public key yet. Let's add one.
	pubKey, encPrivKey, err := keystore.GenerateSealedBoxKeys(d.env)
	if err != nil {
		return "", err
	}

	err = d.h.NewQuery(ctx, "userdb_update_encryption_key").Raw(
		`UPDATE "Groups" SET public_key = ?, encrypted_private_key = ? WHERE group_id = ?`,
		pubKey, encPrivKey, groupID).Exec().Error
	if err != nil {
		return "", err
	}

	return pubKey, nil
}

func getEmailDomain(email string) string {
	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return ""
	}
	return parts[len(parts)-1]
}
