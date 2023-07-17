package userdb

import (
	"context"
	"flag"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

	// Capabilities assigned to user-owned API keys.
	userAPIKeyCapabilities = []akpb.ApiKey_Capability{
		akpb.ApiKey_CAS_WRITE_CAPABILITY,
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
		GroupID:    strings.Replace(u.UserID, "US", "GR", 1),
		UserID:     u.UserID,
		Name:       name,
		WriteToken: randomToken(10),
	}, nil
}

func randomToken(length int) string {
	// NB: Keep in sync with BuildBuddyServer#redactAPIKeys, which relies on this exact impl.
	token, err := random.RandomString(length)
	if err != nil {
		token = "bUiLdBuDdy"
	}
	return token
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
	return d.getGroupByID(d.h.DB(ctx), groupID)
}

func (d *UserDB) getGroupByID(tx *db.DB, groupID string) (*tables.Group, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be empty.")
	}
	query := tx.Raw(`SELECT * FROM "Groups" AS g WHERE g.group_id = ?`, groupID)
	group := &tables.Group{}
	if err := query.Take(group).Error; err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("The requested organization was not found.")
		}
		return nil, err
	}
	return group, nil
}

func (d *UserDB) GetGroupByURLIdentifier(ctx context.Context, urlIdentifier string) (*tables.Group, error) {
	var group *tables.Group
	err := d.h.Transaction(ctx, func(tx *db.DB) error {
		g, err := d.getGroupByURLIdentifier(ctx, tx, urlIdentifier)
		if err != nil {
			return err
		}
		group = g
		return nil
	})
	return group, err
}

func (d *UserDB) getGroupByURLIdentifier(ctx context.Context, tx *db.DB, urlIdentifier string) (*tables.Group, error) {
	if urlIdentifier == "" {
		return nil, status.InvalidArgumentError("URL identifier cannot be empty.")
	}
	query := tx.Raw(`SELECT * FROM "Groups" AS g WHERE g.url_identifier = ?`, urlIdentifier)
	group := &tables.Group{}
	if err := query.Take(group).Error; err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("The requested organization was not found.")
		}
		return nil, err
	}
	return group, nil
}

func (d *UserDB) DeleteUserGitHubToken(ctx context.Context) error {
	u, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return err
	}
	if u.GetUserID() == "" {
		return status.FailedPreconditionError("user ID must not be empty")
	}
	return d.h.DB(ctx).Exec(
		`UPDATE "Users" SET github_token = '' WHERE user_id = ?`,
		u.GetUserID(),
	).Error
}

func (d *UserDB) authorizeGroupAdminRole(ctx context.Context, groupID string) error {
	u, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return err
	}
	return authutil.AuthorizeGroupRole(u, groupID, role.Admin)
}

func isInOwnedDomainBlocklist(email string) bool {
	for _, item := range blockList {
		if item == email {
			return true
		}
	}
	return false
}

func (d *UserDB) getDomainOwnerGroup(ctx context.Context, tx *db.DB, domain string) (*tables.Group, error) {
	tg := &tables.Group{}
	existingRow := tx.Raw(`SELECT * FROM "Groups" as g
                               WHERE g.owned_domain = ?`, domain)
	err := existingRow.Take(tg).Error
	if db.IsRecordNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return tg, nil
}

func getUserGroup(tx *db.DB, userID string, groupID string) (*tables.UserGroup, error) {
	userGroup := &tables.UserGroup{}
	query := tx.Raw(`SELECT * FROM "UserGroups" AS ug
                    WHERE ug.user_user_id = ? AND ug.group_group_id = ?`, userID, groupID)
	if err := query.Take(userGroup).Error; err != nil {
		if db.IsRecordNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return userGroup, nil
}

func (d *UserDB) CreateGroup(ctx context.Context, g *tables.Group) (string, error) {
	u, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return "", err
	}
	groupID := ""
	err = d.h.Transaction(ctx, func(tx *db.DB) error {
		gid, err := d.createGroup(ctx, tx, u.GetUserID(), g)
		groupID = gid
		return err
	})
	if err != nil {
		return "", err
	}
	return groupID, nil
}

func (d *UserDB) createGroup(ctx context.Context, tx *db.DB, userID string, g *tables.Group) (string, error) {
	if g.GroupID != "" {
		return "", status.InvalidArgumentErrorf("cannot create a Group using an existing group ID")
	}
	newGroup := *g // copy
	groupID, err := tables.PrimaryKeyForTable("Groups")
	if err != nil {
		return "", err
	}
	newGroup.GroupID = groupID
	newGroup.WriteToken = randomToken(10)

	if err := tx.Create(&newGroup).Error; err != nil {
		return "", err
	}
	// Initialize the group with a group-owned key.
	_, err = d.env.GetAuthDB().CreateAPIKeyWithoutAuthCheck(tx, groupID, defaultAPIKeyLabel, defaultAPIKeyCapabilities, false /*visibleToDevelopers*/)
	if err != nil {
		return "", err
	}
	if err = d.addUserToGroup(tx, userID, groupID); err != nil {
		return "", err
	}
	return groupID, nil
}

func (d *UserDB) InsertOrUpdateGroup(ctx context.Context, g *tables.Group) (string, error) {
	u, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return "", err
	}
	if isInOwnedDomainBlocklist(g.OwnedDomain) {
		return "", status.InvalidArgumentError("This domain is not allowed to be owned by any group.")
	}
	if g.URLIdentifier == nil {
		return "", status.InvalidArgumentError("Invalid organization URL.")
	}
	if match := groupUrlIdentifierPattern.MatchString(*g.URLIdentifier); !match {
		return "", status.InvalidArgumentError("Invalid organization URL.")
	}
	groupID := ""
	err = d.h.Transaction(ctx, func(tx *db.DB) error {
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
		if err := authutil.AuthorizeGroupRole(u, groupID, role.Admin); err != nil {
			return err
		}

		res := tx.Exec(`
			UPDATE "Groups" SET
				name = ?,
				url_identifier = ?,
				owned_domain = ?,
				sharing_enabled = ?,
				user_owned_keys_enabled = ?,
				use_group_owned_executors = ?,
				cache_encryption_enabled = ?,
				suggestion_preference = ?
			WHERE group_id = ?`,
			g.Name,
			g.URLIdentifier,
			g.OwnedDomain,
			g.SharingEnabled,
			g.UserOwnedKeysEnabled,
			g.UseGroupOwnedExecutors,
			g.CacheEncryptionEnabled,
			g.SuggestionPreference,
			g.GroupID)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return status.NotFoundErrorf("Group %s not found", groupID)
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
	return d.h.DB(ctx).Exec(q, args...).Error
}

func (d *UserDB) addUserToGroup(tx *db.DB, userID, groupID string) (retErr error) {
	// Count the number of users in the group.
	// If there are no existing users, then user should join with admin role,
	// otherwise they should join with provided role.
	row := &struct{ Count int64 }{}
	err := tx.Raw(`
		SELECT COUNT(*) AS count FROM "UserGroups"
		WHERE group_group_id = ? AND membership_status = ?
	`, groupID, grpb.GroupMembershipStatus_MEMBER).Take(row).Error
	if err != nil {
		return err
	}
	r := role.Default
	if row.Count == 0 {
		r = role.Admin
	}

	existing, err := getUserGroup(tx, userID, groupID)
	if err != nil && !db.IsRecordNotFound(err) {
		return err
	}
	if existing != nil {
		if existing.MembershipStatus == int32(grpb.GroupMembershipStatus_REQUESTED) {
			return tx.Exec(`
				UPDATE "UserGroups"
				SET membership_status = ?, role = ?
				WHERE user_user_id = ?
				AND group_group_id = ?
				`,
				grpb.GroupMembershipStatus_MEMBER,
				r,
				userID,
				groupID,
			).Error
		}
		return status.AlreadyExistsError("You're already in this organization.")
	}

	return tx.Exec(
		`INSERT INTO "UserGroups" (user_user_id, group_group_id, membership_status, role) VALUES(?, ?, ?, ?)`,
		userID, groupID, int32(grpb.GroupMembershipStatus_MEMBER), r,
	).Error
}

func (d *UserDB) RequestToJoinGroup(ctx context.Context, groupID string) (grpb.GroupMembershipStatus, error) {
	u, err := perms.AuthenticatedUser(ctx, d.env)
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
	err = d.h.Transaction(ctx, func(tx *db.DB) error {
		tu, err := d.getUser(tx, u.GetUserID())
		if err != nil {
			return err
		}
		group, err := d.getGroupByID(tx, groupID)
		if err != nil {
			return err
		}
		// If the org has an owned domain that matches the user's email,
		// the user can join directly as a member.
		membershipStatus = grpb.GroupMembershipStatus_REQUESTED
		if group.OwnedDomain != "" && group.OwnedDomain == getEmailDomain(tu.Email) {
			membershipStatus = grpb.GroupMembershipStatus_MEMBER
			return d.addUserToGroup(tx, userID, groupID)
		}

		// Check if there's an existing request and return AlreadyExists if so.
		existing, err := getUserGroup(tx, userID, groupID)
		if err != nil {
			return err
		}
		if existing != nil {
			if existing.MembershipStatus == int32(grpb.GroupMembershipStatus_REQUESTED) {
				return status.AlreadyExistsError("You've already requested to join this organization.")
			}
			return status.AlreadyExistsError("You're already in this organization.")
		}
		return tx.Create(&tables.UserGroup{
			UserUserID:       userID,
			GroupGroupID:     groupID,
			Role:             uint32(role.Default),
			MembershipStatus: int32(membershipStatus),
		}).Error
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
	rows, err := d.h.DB(ctx).Raw(qString, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		groupUser := &grpb.GetGroupUsersResponse_GroupUser{}
		user := &tables.User{}
		var groupRole uint32
		err := rows.Scan(
			&user.UserID, &user.Email, &user.FirstName, &user.LastName,
			&groupUser.GroupMembershipStatus, &groupRole,
		)
		if err != nil {
			return nil, err
		}
		groupUser.User = user.ToProto()
		groupUser.Role = role.ToProto(role.Role(groupRole))
		users = append(users, groupUser)
	}

	return users, nil
}

func (d *UserDB) removeUserFromGroup(ctx context.Context, tx *db.DB, userID string, groupID string) error {
	ug, err := getUserGroup(tx, userID, groupID)
	if err != nil {
		return err
	}
	if ug == nil {
		return nil
	}
	if err := tx.Exec(`
						DELETE FROM "UserGroups"
						WHERE user_user_id = ? AND group_group_id = ?`,
		userID,
		groupID).Error; err != nil {
		return err
	}
	if err := tx.Exec(`
						DELETE FROM "APIKeys"
						WHERE user_id = ? AND group_id = ?`,
		userID,
		groupID).Error; err != nil {
		return err
	}
	return nil
}

func (d *UserDB) updateUserRole(ctx context.Context, tx *db.DB, userID string, groupID string, newRole role.Role) error {
	ug, err := getUserGroup(tx, userID, groupID)
	if err != nil {
		return err
	}
	if ug == nil {
		return nil
	}
	if role.Role(ug.Role) == newRole {
		return nil
	}
	err = tx.Exec(`
					UPDATE "UserGroups"
					SET role = ?
					WHERE user_user_id = ? AND group_group_id = ?
				`, newRole, userID, groupID,
	).Error
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
	return d.h.Transaction(ctx, func(tx *db.DB) error {
		for _, update := range updates {
			switch update.GetMembershipAction() {
			case grpb.UpdateGroupUsersRequest_Update_REMOVE:
				if err := d.removeUserFromGroup(ctx, tx, update.GetUserId().GetId(), groupID); err != nil {
					return err
				}
			case grpb.UpdateGroupUsersRequest_Update_ADD:
				if err := d.addUserToGroup(tx, update.GetUserId().GetId(), groupID); err != nil {
					return err
				}
			case grpb.UpdateGroupUsersRequest_Update_UNKNOWN_MEMBERSHIP_ACTION:
			default:
				return status.InvalidArgumentError("Invalid membership action")
			}

			if update.Role != grpb.Group_UNKNOWN_ROLE {
				if err := d.updateUserRole(ctx, tx, update.GetUserId().GetId(), groupID, role.FromProto(update.GetRole())); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (d *UserDB) CreateDefaultGroup(ctx context.Context) error {
	return d.h.Transaction(ctx, func(tx *db.DB) error {
		var existing tables.Group
		if err := tx.Where("group_id = ?", DefaultGroupID).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				g := d.getDefaultGroupConfig()
				if g.WriteToken == "" {
					g.WriteToken = randomToken(10)
				}
				if err := tx.Create(g).Error; err != nil {
					return err
				}
				if _, err := d.env.GetAuthDB().CreateAPIKeyWithoutAuthCheck(tx, DefaultGroupID, defaultAPIKeyLabel, defaultAPIKeyCapabilities, false /*visibleToDevelopers*/); err != nil {
					return err
				}
				return nil
			}
			return err
		}

		return tx.Model(&tables.Group{}).Where("group_id = ?", DefaultGroupID).Updates(d.getDefaultGroupConfig()).Error
	})
}

func (d *UserDB) getDefaultGroupConfig() *tables.Group {
	return &tables.Group{
		GroupID:     DefaultGroupID,
		Name:        *orgName,
		OwnedDomain: *orgDomain,
	}
}

func (d *UserDB) createUser(ctx context.Context, tx *db.DB, u *tables.User) error {
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
		hydratedGroup, err := d.getGroupByURLIdentifier(ctx, tx, *group.Group.URLIdentifier)
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
		if err := tx.Create(&sug).Error; err != nil {
			return err
		}
		// For now, user-owned groups are assigned an org-level API key, and
		// users have to explicitly enable user-owned keys.
		if _, err := d.env.GetAuthDB().CreateAPIKeyWithoutAuthCheck(tx, sug.GroupID, defaultAPIKeyLabel, defaultAPIKeyCapabilities, false /*visibleToDevelopers*/); err != nil {
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

	err := tx.Create(u).Error
	if err != nil {
		return err
	}

	for _, groupID := range groupIDs {
		err := tx.Exec(`
			INSERT INTO "UserGroups" (user_user_id, group_group_id, membership_status, role)
			VALUES (?, ?, ?, ?)
			`, u.UserID, groupID, int32(grpb.GroupMembershipStatus_MEMBER), uint32(role.Default),
		).Error
		if err != nil {
			return err
		}
		// Promote from default role to admin if the user is the only one in the
		// group after joining.
		preExistingUsers := &struct{ Count int64 }{}
		err = tx.Raw(`
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
		err = tx.Exec(`
			UPDATE "UserGroups"
			SET role = ?
			WHERE group_group_id = ?
			`, uint32(role.Admin), groupID,
		).Error
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *UserDB) InsertUser(ctx context.Context, u *tables.User) error {
	return d.h.Transaction(ctx, func(tx *db.DB) error {
		var existing tables.User
		if err := tx.Where("sub_id = ?", u.SubID).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				return d.createUser(ctx, tx, u)
			}
			return err
		}
		return status.FailedPreconditionError("User already exists!")
	})
}

func (d *UserDB) GetUser(ctx context.Context) (*tables.User, error) {
	auth := d.env.GetAuthenticator()
	if auth == nil {
		return nil, status.InternalError("No auth configured on this BuildBuddy instance")
	}
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	var user *tables.User
	err = d.h.Transaction(ctx, func(tx *db.DB) error {
		user, err = d.getUser(tx, u.GetUserID())
		return err
	})
	return user, err
}

func (d *UserDB) getUser(tx *db.DB, userID string) (*tables.User, error) {
	user := &tables.User{}
	userRow := tx.Raw(`SELECT * FROM "Users" WHERE user_id = ?`, userID)
	if err := userRow.Take(user).Error; err != nil {
		return nil, err
	}
	groupRows, err := tx.Raw(`
		SELECT
			g.user_id,
			g.group_id,
			g.url_identifier,
			g.name,
			g.owned_domain,
			g.github_token,
			g.sharing_enabled,
			g.user_owned_keys_enabled,
			g.use_group_owned_executors,
			g.cache_encryption_enabled,
			g.saml_idp_metadata_url,
			g.suggestion_preference,
			ug.role
		FROM "Groups" as g
		JOIN "UserGroups" as ug
		ON g.group_id = ug.group_group_id
		WHERE ug.user_user_id = ? AND ug.membership_status = ?
	`, userID, int32(grpb.GroupMembershipStatus_MEMBER)).Rows()
	if err != nil {
		return nil, err
	}
	defer groupRows.Close()
	for groupRows.Next() {
		gr := &tables.GroupRole{}
		err := groupRows.Scan(
			// NOTE: When updating the group fields here, update GetImpersonatedUser
			// as well.
			&gr.Group.UserID,
			&gr.Group.GroupID,
			&gr.Group.URLIdentifier,
			&gr.Group.Name,
			&gr.Group.OwnedDomain,
			&gr.Group.GithubToken,
			&gr.Group.SharingEnabled,
			&gr.Group.UserOwnedKeysEnabled,
			&gr.Group.UseGroupOwnedExecutors,
			&gr.Group.CacheEncryptionEnabled,
			&gr.Group.SamlIdpMetadataUrl,
			&gr.Group.SuggestionPreference,
			&gr.Role,
		)
		if err != nil {
			return nil, err
		}
		user.Groups = append(user.Groups, gr)
	}
	return user, nil
}

func (d *UserDB) GetImpersonatedUser(ctx context.Context) (*tables.User, error) {
	auth := d.env.GetAuthenticator()
	if auth == nil {
		return nil, status.InternalError("No auth configured on this BuildBuddy instance")
	}
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if !u.IsImpersonating() {
		return nil, status.PermissionDeniedError("Authenticated user does not have permissions to impersonate a user.")
	}
	user := &tables.User{}
	err = d.h.Transaction(ctx, func(tx *db.DB) error {
		userRow := tx.Raw(`SELECT * FROM "Users" WHERE user_id = ?`, u.GetUserID())
		if err := userRow.Take(user).Error; err != nil {
			return err
		}
		groupRows, err := tx.Raw(`
			SELECT
				user_id,
				group_id,
				url_identifier,
				name,
				owned_domain,
				github_token,
				sharing_enabled,
				user_owned_keys_enabled,
				use_group_owned_executors,
				cache_encryption_enabled,
				saml_idp_metadata_url,
				suggestion_preference
			FROM "Groups"
			WHERE group_id = ?
		`, u.GetGroupID()).Rows()
		if err != nil {
			return err
		}
		defer groupRows.Close()
		for groupRows.Next() {
			gr := &tables.GroupRole{}
			err := groupRows.Scan(
				&gr.Group.UserID,
				&gr.Group.GroupID,
				&gr.Group.URLIdentifier,
				&gr.Group.Name,
				&gr.Group.OwnedDomain,
				&gr.Group.GithubToken,
				&gr.Group.SharingEnabled,
				&gr.Group.UserOwnedKeysEnabled,
				&gr.Group.UseGroupOwnedExecutors,
				&gr.Group.CacheEncryptionEnabled,
				&gr.Group.SamlIdpMetadataUrl,
				&gr.Group.SuggestionPreference,
			)
			if err != nil {
				return err
			}
			// Grant admin role within the impersonated group.
			gr.Role = uint32(role.Admin)
			user.Groups = append(user.Groups, gr)
		}
		return nil
	})
	return user, err
}

func (d *UserDB) FillCounts(ctx context.Context, stat *telpb.TelemetryStat) error {
	counts := d.h.DB(ctx).Raw(`
		SELECT 
			COUNT(DISTINCT user_id) as registered_user_count
		FROM "Users" as u
		WHERE 
			u.created_at_usec >= ? AND
			u.created_at_usec < ?`,
		time.Now().Truncate(24*time.Hour).Add(-24*time.Hour).UnixMicro(),
		time.Now().Truncate(24*time.Hour).UnixMicro())

	if err := counts.Take(stat).Error; err != nil {
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

	err = d.h.DB(ctx).Exec(
		`UPDATE "Groups" SET public_key = ?, encrypted_private_key = ? WHERE group_id = ?`,
		pubKey, encPrivKey, groupID).Error
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
