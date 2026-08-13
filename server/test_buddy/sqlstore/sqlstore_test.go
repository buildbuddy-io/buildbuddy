package sqlstore_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/sqlstore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
)

func TestSchemaIsOwnedByTestBuddy(t *testing.T) {
	ctx := context.Background()
	database := testenv.GetTestEnv(t).GetDBHandle()
	require.False(t, database.GORM(ctx, "test_buddy_schema_before_migrate").Migrator().HasTable("Tests"))
	require.NoError(t, sqlstore.Migrate(ctx, database))
	for _, table := range []string{"Tests", "TestAnalyzerConfigs", "TestStateChanges"} {
		require.True(t, database.GORM(ctx, "test_buddy_schema_after_migrate").Migrator().HasTable(table), table)
	}
}
