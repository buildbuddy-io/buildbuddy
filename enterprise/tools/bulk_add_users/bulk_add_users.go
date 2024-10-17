package main

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	sourceGroupID = flag.String("source_group_id", "", "Group to copy UserGroups from")
	targetGroupID = flag.String("target_group_id", "", "Group to copy UserGroups to")
	dryRun        = flag.Bool("dry_run", true, "Dry-run mode")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	if err := flagutil.SetValueForFlagName("auto_migrate_db", false, nil, false); err != nil {
		return err
	}

	if *sourceGroupID == "" || *targetGroupID == "" {
		return fmt.Errorf("must set both -source_group_id and -target_group_id")
	}

	ctx := context.Background()
	env := real_environment.NewBatchEnv()
	dbh, err := db.GetConfiguredDatabase(ctx, env)
	if err != nil {
		return fmt.Errorf("configure db: %w", err)
	}

	q := dbh.NewQuery(ctx, "bulk_add_users_select").Raw(`
		SELECT UG.*
		FROM "Users" U, "UserGroups" UG
		WHERE UG.user_user_id = U.user_id
		AND group_group_id = ?
		AND NOT EXISTS (
			SELECT * FROM "UserGroups"
			WHERE group_group_id = ?
			AND user_user_id = UG.user_user_id
		)
	`, *sourceGroupID, *targetGroupID)
	ugs, err := db.ScanAll(q, &tables.UserGroup{})
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	for _, ug := range ugs {
		ug.GroupGroupID = *targetGroupID

		u := &tables.User{}
		if err := dbh.NewQuery(ctx, "bulk_add_users_get_user").Raw(`
			SELECT U.*
			FROM "Users" U
			WHERE user_id = ?
		`, ug.UserUserID).Take(u); err != nil {
			return fmt.Errorf("get user: %w", err)
		}

		fmt.Printf("%-25s\t%#+v\n", u.Email, *ug)

		if *dryRun {
			continue
		}

		if err := dbh.NewQuery(ctx, "bulk_add_users_insert").Create(ug); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
	}

	return nil
}
