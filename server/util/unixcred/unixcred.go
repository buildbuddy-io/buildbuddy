// package unixcred provides facilities for dealing with unix user entries in
// /etc/passwd and group entries in /etc/group.
package unixcred

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

var (
	// ErrUserNotFound is returned by LookupUser when no matching entry is
	// found.
	ErrUserNotFound = fmt.Errorf("user not found")

	// ErrGroupNotFound is returned by LookupGroup when no matching entry is
	// found.
	ErrGroupNotFound = fmt.Errorf("group not found")
)

// NameOrID specifies a user or group either by name or numeric ID.
//
// If Name is empty then ID specifies a numeric uid or gid.
// Otherwise, ID should be ignored and the ID will be
// looked up from /etc/passwd.
type NameOrID struct {
	// Name is the string name that should be looked up from /etc/passwd.
	Name string

	// ID is the numeric ID, if a number was specified.
	ID uint32
}

func (n *NameOrID) String() string {
	if n.Name != "" {
		return n.Name
	}
	return fmt.Sprintf("%d", n.ID)
}

// UserRecord represents a line in the /etc/passwd file.
type UserRecord struct {
	Username string
	Password string
	UID      uint32
	GID      uint32
	UserInfo string
	HomeDir  string
	Shell    string
}

// parseUserRecord parses a line from /etc/passwd into a UserRecord struct.
func parseUserRecord(line string) (*UserRecord, error) {
	fields := strings.Split(line, ":")
	if len(fields) != 7 {
		return nil, fmt.Errorf("invalid line: %s", line)
	}
	uid, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid UID: %s", fields[2])
	}
	gid, err := strconv.ParseUint(fields[3], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid GID: %s", fields[3])
	}
	return &UserRecord{
		Username: fields[0],
		Password: fields[1],
		UID:      uint32(uid),
		GID:      uint32(gid),
		UserInfo: fields[4],
		HomeDir:  fields[5],
		Shell:    fields[6],
	}, nil
}

// LookupUser looks for the given user name or ID in the given /etc/passwd file
// and returns the user record if found. It returns ErrUserNotFound if no such
// user exists.
func LookupUser(etcPasswdPath string, user *NameOrID) (*UserRecord, error) {
	f, err := os.Open(etcPasswdPath)
	if err != nil {
		return nil, fmt.Errorf("open /etc/passwd: %w", err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		record, err := parseUserRecord(line)
		if err != nil {
			continue
		}
		if (user.Name != "" && user.Name == record.Username) ||
			(user.Name == "" && user.ID == record.UID) {
			return record, nil
		}
	}
	if s.Err() != nil {
		return nil, fmt.Errorf("read /etc/passwd: %w", err)
	}
	return nil, ErrUserNotFound
}

// GroupRecord represents a line in the /etc/group file.
type GroupRecord struct {
	Name     string
	Password string
	GID      uint32
	Users    []string
}

// parseGroupRecord parses a single line from the group file into a GroupRecord.
func parseGroupRecord(line string) (*GroupRecord, error) {
	fields := strings.Split(line, ":")
	if len(fields) != 4 {
		return nil, fmt.Errorf("invalid group record")
	}
	groupID, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return nil, err
	}
	var users []string
	if fields[3] != "" {
		users = strings.Split(fields[3], ",")
	}
	return &GroupRecord{
		Name:     fields[0],
		Password: fields[1],
		GID:      uint32(groupID),
		Users:    users,
	}, nil
}

// LookupGroup looks for the given group name or ID in the given /etc/group file
// and returns the group record if found. It returns ErrGroupNotFound if no such
// group exists.
func LookupGroup(etcGroupPath string, group *NameOrID) (*GroupRecord, error) {
	f, err := os.Open(etcGroupPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}
		groupRecord, err := parseGroupRecord(line)
		if err != nil {
			continue
		}
		if (group.Name != "" && group.Name == groupRecord.Name) ||
			(group.Name == "" && group.ID == groupRecord.GID) {
			return groupRecord, nil
		}
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("read /etc/group: %w", err)
	}
	return nil, ErrGroupNotFound
}

// GetGroupsWithUser returns all entries in the given /etc/group file where
// the given username is explicitly listed as a member of the group.
func GetGroupsWithUser(etcGroupPath, username string) ([]*GroupRecord, error) {
	var out []*GroupRecord
	f, err := os.Open(etcGroupPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}
		groupRecord, err := parseGroupRecord(line)
		if err != nil {
			continue
		}
		if slices.Contains(groupRecord.Users, username) {
			out = append(out, groupRecord)
		}
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("read /etc/group: %w", err)
	}
	return out, nil
}
