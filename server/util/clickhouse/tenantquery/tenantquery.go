// Package tenantquery configures ClickHouse to run SQL that tenants write.
//
// To isolate tenants, the app does not parse or rewrite the SQL. Instead, it
// runs every tenant query as a dedicated ClickHouse user, and ClickHouse limits
// what that user can read and how many resources it can use (see
// schema.EnsureTenantAccess). RunMigrations applies the limit flags to
// ClickHouse each time that it runs.
package tenantquery

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"gorm.io/gorm"
)

var (
	dataSource       = flag.String("olap_database.tenant_queries.data_source", "", "HTTP URL of the ClickHouse server that runs tenant queries, like http://user:password@host:8123/database. Auto-migration creates the user in the URL and only lets it read the rows of the group that sends each query. Tenant queries are disabled if this is empty.", flag.Secret)
	maxExecutionTime = flag.Duration("olap_database.tenant_queries.max_execution_time", 60*time.Second, "Maximum time that a tenant query may run.")
	maxThreads       = flag.Int64("olap_database.tenant_queries.max_threads", 4, "Maximum number of threads, and therefore CPU cores, that a tenant query may use.")
	maxMemoryBytes   = flag.Int64("olap_database.tenant_queries.max_memory_bytes", 4_000_000_000, "Maximum amount of memory that a tenant query may use.")
	maxResultRows    = flag.Int64("olap_database.tenant_queries.max_result_rows", 10_000, "Maximum number of rows that a tenant query may return.")
	maxResultBytes   = flag.Int64("olap_database.tenant_queries.max_result_bytes", 16_000_000, "Maximum size of the result that a tenant query may return.")

	// parseDataSource only accepts user names that match identifierRE, because
	// schema.EnsureTenantAccess puts the user name in SQL statements without
	// quoting it.
	identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// RunMigrations creates the ClickHouse user in dataSource, and limits what the
// user can read and how many resources it can use (see
// schema.EnsureTenantAccess). It does nothing if dataSource is empty.
func RunMigrations(gdb *gorm.DB) error {
	if *dataSource == "" {
		return nil
	}
	u, err := parseDataSource()
	if err != nil {
		return err
	}
	// Hash the password here to keep the plaintext password out of SQL
	// statements. ClickHouse authenticates the user against this SHA-256 hash.
	password, _ := u.User.Password()
	passwordHash := sha256.Sum256([]byte(password))
	err = schema.EnsureTenantAccess(gdb, &schema.TenantAccess{
		User:             u.User.Username(),
		PasswordSHA256:   hex.EncodeToString(passwordHash[:]),
		MaxExecutionTime: *maxExecutionTime,
		MaxThreads:       *maxThreads,
		MaxMemoryBytes:   *maxMemoryBytes,
		MaxResultRows:    *maxResultRows,
		MaxResultBytes:   *maxResultBytes,
	})
	if err != nil {
		return fmt.Errorf("ensure tenant access: %w", err)
	}
	return nil
}

func parseDataSource() (*url.URL, error) {
	u, err := url.Parse(*dataSource)
	if err != nil {
		// Leave out err, because url.Parse includes dataSource and its
		// password in the error.
		return nil, errors.New("tenant data source is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("tenant data source must be an http or https URL")
	}
	if !identifierRE.MatchString(u.User.Username()) {
		return nil, errors.New("tenant data source must have a user name made of letters, digits and underscores")
	}
	if password, _ := u.User.Password(); password == "" {
		return nil, errors.New("tenant data source must have a password")
	}
	if strings.Trim(u.Path, "/") == "" {
		return nil, errors.New("tenant data source must name a database")
	}
	return u, nil
}
