package tables_test

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/schema"
)

func TestBuddyTablesUseNaturalAddressKeys(t *testing.T) {
	for _, test := range []struct {
		table any
		want  []string
	}{
		{&tables.TestRepositoryCatalog{}, []string{"group_id", "repository"}},
		{&tables.TestTarget{}, []string{"group_id", "repository", "target_label"}},
		{&tables.TestCase{}, []string{"group_id", "repository", "target_label", "case_name"}},
		{&tables.TestPackageCoverage{}, []string{"group_id", "repository", "package_path"}},
		{&tables.TestAnalyzerConfig{}, []string{"group_id", "repository"}},
		{&tables.TestTargetState{}, []string{"group_id", "repository", "target_label"}},
		{&tables.TestCaseState{}, []string{"group_id", "repository", "target_label", "case_name"}},
		{&tables.TestTargetStateChange{}, []string{"group_id", "repository", "target_label", "state_version"}},
		{&tables.TestCaseStateChange{}, []string{"group_id", "repository", "target_label", "case_name", "state_version"}},
		{&tables.TestFailureCluster{}, []string{"group_id", "repository", "fingerprint"}},
	} {
		parsed, err := schema.Parse(test.table, &sync.Map{}, schema.NamingStrategy{})
		require.NoError(t, err)
		primary := make([]string, 0, len(parsed.PrimaryFields))
		for _, field := range parsed.PrimaryFields {
			primary = append(primary, field.DBName)
		}
		assert.Equal(t, test.want, primary)
		assert.NotContains(t, primary, "test_case_id")
		assert.NotContains(t, primary, "target_id")
		assert.NotContains(t, primary, "repo_key")
	}
}

func TestBuddyCaseKeyColumnsFitEncodedNames(t *testing.T) {
	for _, table := range []any{
		&tables.TestCase{}, &tables.TestCaseState{}, &tables.TestCaseStateChange{},
	} {
		parsed, err := schema.Parse(table, &sync.Map{}, schema.NamingStrategy{})
		require.NoError(t, err)
		field := parsed.LookUpField("CaseName")
		require.NotNil(t, field)
		assert.Equal(t, identity.MaxCaseNameKeyBytes, field.Size)
	}
}

func TestBuddyTableNames(t *testing.T) {
	assert.ElementsMatch(t, []string{
		"TestRepositoryCatalogs",
		"TestTargets",
		"TestCases",
		"TestPackageCoverages",
		"TestAnalyzerConfigs",
		"TestCaseStates",
		"TestTargetStates",
		"TestCaseStateChanges",
		"TestTargetStateChanges",
		"TestFailureClusters",
	}, tables.TestBuddyTableNames())
}

func TestBuddyAddressColumnsAreASCIIOnMySQL(t *testing.T) {
	ctx := context.Background()
	dbh := testenv.GetTestEnv(t).GetDBHandle()
	if dbh.DialectName() != "mysql" {
		t.Skip("MySQL-only schema check")
	}
	type column struct {
		Table     string `gorm:"column:table_name"`
		Name      string `gorm:"column:column_name"`
		Charset   string `gorm:"column:character_set_name"`
		Collation string `gorm:"column:collation_name"`
		InKey     int    `gorm:"column:in_key"`
	}
	var columns []column
	require.NoError(t, dbh.GORM(ctx, "test_buddy_column_charsets").Raw(`
		SELECT c.table_name AS table_name,
		       c.column_name AS column_name,
		       c.character_set_name AS character_set_name,
		       c.collation_name AS collation_name,
		       EXISTS (
		         SELECT 1 FROM information_schema.statistics s
		          WHERE s.table_schema = c.table_schema
		            AND s.table_name = c.table_name
		            AND s.column_name = c.column_name
		       ) AS in_key
		  FROM information_schema.columns c
		 WHERE c.table_schema = DATABASE()
		   AND c.table_name IN ?
		   AND c.character_set_name IS NOT NULL`,
		tables.TestBuddyTableNames()).Scan(&columns).Error)
	require.NotEmpty(t, columns)
	for _, column := range columns {
		if column.InKey != 0 {
			assert.Equal(t, "ascii", column.Charset, "%s.%s", column.Table, column.Name)
			assert.Equal(t, "ascii_bin", column.Collation, "%s.%s", column.Table, column.Name)
		}
	}
}
