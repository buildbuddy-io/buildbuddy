package ociuser

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/server/util/unixcred"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// Resolve resolves a "USER[:GROUP]" string into a specs.User struct given a
// rootfs path. If the rootfs path contains /etc/passwd or /etc/group entries
// then they are used to resolve user or group names to numeric IDs.
func Resolve(input string, rootfsPath string) (*specs.User, error) {
	user, group, err := unixcred.ParseUserGroup(input)
	if err != nil {
		return nil, fmt.Errorf(`invalid "USER[:GROUP]" spec %q: %w`, input, err)
	}

	var uid, gid uint32
	username := user.Name

	// If the user is non-numeric then we need to look it up from /etc/passwd.
	// If no gid is specified then we need to find the user entry in /etc/passwd
	// to know what group they are in.
	if user.Name != "" || group == nil {
		userRecord, err := unixcred.LookupUser(filepath.Join(rootfsPath, "/etc/passwd"), user)
		if (err == unixcred.ErrUserNotFound || os.IsNotExist(err)) && user.Name == "" {
			// If no user was found in /etc/passwd and we specified only a
			// numeric user ID then just set the group ID to 0 (root). This is
			// what docker/podman do, presumably because it's usually safe to
			// assume that gid 0 exists.
			uid = user.ID
			gid = 0
		} else if err != nil {
			return nil, fmt.Errorf("lookup user %q in /etc/passwd: %w", user, err)
		} else {
			uid = userRecord.UID
			username = userRecord.Username
			if group == nil {
				gid = userRecord.GID
			}
		}
	} else {
		uid = user.ID
	}

	if group != nil {
		// If a group was specified by name then look it up from /etc/group.
		if group.Name != "" {
			groupRecord, err := unixcred.LookupGroup(filepath.Join(rootfsPath, "/etc/group"), user)
			if err != nil {
				return nil, fmt.Errorf("lookup group %q in /etc/group: %w", group, err)
			}
			gid = groupRecord.GID
		} else {
			gid = group.ID
		}
	}

	gids := []uint32{gid}

	// If no group is explicitly specified and we have a username, then
	// search /etc/group for additional groups that the user might be in
	// (/etc/group lists members by username, not by uid).
	if group == nil && username != "" {
		groups, err := unixcred.GetGroupsWithUser(filepath.Join(rootfsPath, "/etc/group"), username)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("lookup groups with user %q in /etc/passwd: %w", username, err)
		}
		for _, g := range groups {
			gids = append(gids, g.GID)
		}
	}
	slices.Sort(gids)
	gids = slices.Compact(gids)

	var umask uint32 = 0o022 // 0644 file perms by default
	return &specs.User{
		UID:            uid,
		GID:            gid,
		AdditionalGids: gids,
		Umask:          &umask,
	}, nil
}
