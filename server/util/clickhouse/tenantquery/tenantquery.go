// Package tenantquery runs SQL that tenants write against ClickHouse.
//
// We do not parse or rewrite the SQL in order to isolate tenants. Instead, we
// run every query as a dedicated ClickHouse user, and ClickHouse limits what
// that user can read and how many resources its queries can use (see
// schema.EnsureTenantAccess). The limits depend on settings that we send with
// every query, namely the authenticated group ID and the readonly mode that
// stops the query from changing its own settings.
//
// Auto-migration applies the limit flags to ClickHouse, so changes to them
// take effect the next time that auto-migration runs.
package tenantquery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gorm.io/gorm"

	chproto "github.com/ClickHouse/ch-go/proto"
	tqpb "github.com/buildbuddy-io/buildbuddy/proto/tenant_query"
)

var (
	dataSource              = flag.String("olap_database.tenant_queries.data_source", "", "HTTP URL of the ClickHouse server that runs tenant queries, like http://user:password@host:8123/database. Auto-migration creates the user in the URL and only lets it read the rows of the group that sends each query. Tenant queries are disabled if this is empty.", flag.Secret)
	maxExecutionTime        = flag.Duration("olap_database.tenant_queries.max_execution_time", 60*time.Second, "Maximum time that a tenant query may run.")
	maxThreads              = flag.Int64("olap_database.tenant_queries.max_threads", 4, "Maximum number of threads, and therefore CPU cores, that a tenant query may use.")
	maxMemoryBytes          = flag.Int64("olap_database.tenant_queries.max_memory_bytes", 4_000_000_000, "Maximum amount of memory that a tenant query may use.")
	maxResultRows           = flag.Int64("olap_database.tenant_queries.max_result_rows", 10_000, "Maximum number of rows that a tenant query may return.")
	maxResultBytes          = flag.Int64("olap_database.tenant_queries.max_result_bytes", 16_000_000, "Maximum size of the result that a tenant query may return.")
	maxConcurrentQueries    = flag.Int64("olap_database.tenant_queries.max_concurrent_queries", 8, "Maximum number of tenant queries that may run at the same time, counting the queries of every group.")
	maxQueriesPerHour       = flag.Int64("olap_database.tenant_queries.max_queries_per_hour", 1000, "Maximum number of tenant queries that each group may run per hour.")
	maxExecutionTimePerHour = flag.Duration("olap_database.tenant_queries.max_execution_time_per_hour", 10*time.Minute, "Maximum total time that the tenant queries of each group may run per hour.")

	// identifierRE matches the user names that we accept in the data source.
	// Auto-migration puts the user name in SQL statements without quoting it.
	identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// versionSuffixRE matches the server version that ClickHouse appends to
	// every error message.
	versionSuffixRE = regexp.MustCompile(`\s*\(version [^()]*(\([^()]*\))?\)\s*$`)
)

// RunMigrations creates the ClickHouse user in the tenant data source, and
// limits what the user can read and how many resources its queries can use
// (see schema.EnsureTenantAccess). It does nothing if tenant queries are
// disabled.
func RunMigrations(gdb *gorm.DB) error {
	if *dataSource == "" {
		return nil
	}
	u, err := parseDataSource()
	if err != nil {
		return err
	}
	// ClickHouse's sha256_hash authentication stores the SHA-256 hash of the
	// password. We compute the hash here so that the password does not appear
	// in the SQL statements that create the user.
	password, _ := u.User.Password()
	passwordHash := sha256.Sum256([]byte(password))
	err = schema.EnsureTenantAccess(gdb, &schema.TenantAccess{
		User:                    u.User.Username(),
		PasswordSHA256:          hex.EncodeToString(passwordHash[:]),
		MaxExecutionTime:        *maxExecutionTime,
		MaxThreads:              *maxThreads,
		MaxMemoryBytes:          *maxMemoryBytes,
		MaxResultRows:           *maxResultRows,
		MaxResultBytes:          *maxResultBytes,
		MaxConcurrentQueries:    *maxConcurrentQueries,
		MaxQueriesPerHour:       *maxQueriesPerHour,
		MaxExecutionTimePerHour: *maxExecutionTimePerHour,
	})
	if err != nil {
		return fmt.Errorf("ensure tenant access: %w", err)
	}
	return nil
}

// Register adds the tenant query service to env. It does nothing if tenant
// queries are disabled.
func Register(env *real_environment.RealEnv) error {
	if *dataSource == "" {
		return nil
	}
	u, err := parseDataSource()
	if err != nil {
		return err
	}
	env.SetTenantQueryService(&service{env: env, dataSource: u})
	return nil
}

func parseDataSource() (*url.URL, error) {
	u, err := url.Parse(*dataSource)
	if err != nil {
		// We leave out the parse error, because it includes the URL, which
		// contains the password.
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

// service implements interfaces.TenantQueryService using ClickHouse's HTTP
// interface. We use HTTP instead of the native protocol that the OLAP DB
// handle uses, because over HTTP, ClickHouse renders the result of any query
// as text.
type service struct {
	env        environment.Env
	dataSource *url.URL
}

// Query runs the query as the group that is authenticated in ctx.
func (s *service) Query(ctx context.Context, req *tqpb.QueryRequest) (*tqpb.QueryResponse, error) {
	user, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := user.GetGroupID()
	if groupID == "" {
		return nil, status.PermissionDeniedError("tenant queries require an authenticated group")
	}
	if strings.TrimSpace(req.GetQuery()) == "" {
		return nil, status.InvalidArgumentError("query is required")
	}

	params := url.Values{}
	params.Set("database", strings.Trim(s.dataSource.Path, "/"))
	params.Set("default_format", "TSVWithNames")
	// The row policy only lets the query read the rows whose group_id equals
	// this setting.
	params.Set(schema.TenantGroupIDSetting, groupID)
	// In readonly mode, ClickHouse rejects queries that change settings, so
	// the query cannot set another group ID in a SETTINGS clause. ClickHouse
	// applies all of the settings in the request before readonly mode takes
	// effect, which is why we can still set the group ID above.
	params.Set("readonly", "1")
	// ClickHouse tracks the usage of the quota separately for each quota key.
	params.Set("quota_key", groupID)
	// Make ClickHouse buffer the whole result before it responds. Otherwise,
	// if the query fails after ClickHouse starts sending the result, we get a
	// successful response with part of the result.
	params.Set("wait_end_of_query", "1")
	queryURL := url.URL{Scheme: s.dataSource.Scheme, Host: s.dataSource.Host, RawQuery: params.Encode()}

	// ClickHouse stops the query after the max execution time. We wait a
	// little longer than that so that we receive the error from ClickHouse.
	ctx, cancel := context.WithTimeout(ctx, *maxExecutionTime+10*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, queryURL.String(), strings.NewReader(req.GetQuery()))
	if err != nil {
		return nil, status.InternalErrorf("create ClickHouse request: %s", err)
	}
	password, _ := s.dataSource.User.Password()
	httpReq.SetBasicAuth(s.dataSource.User.Username(), password)
	rsp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, status.UnavailableErrorf("send query to ClickHouse: %s", err)
	}
	defer rsp.Body.Close()
	// ClickHouse limits the size of the result in memory, but the result can
	// be larger as text, so we enforce the limit here as well.
	body, err := io.ReadAll(io.LimitReader(rsp.Body, *maxResultBytes+1))
	if err != nil {
		return nil, status.UnavailableErrorf("read ClickHouse response: %s", err)
	}
	if rsp.StatusCode != http.StatusOK {
		code, _ := strconv.Atoi(rsp.Header.Get("X-ClickHouse-Exception-Code"))
		return nil, clickhouseError(code, string(body))
	}
	if int64(len(body)) > *maxResultBytes {
		return nil, status.ResourceExhaustedErrorf("query result is larger than %d bytes", *maxResultBytes)
	}
	// ClickHouse sends the headers after the query finishes, because of
	// wait_end_of_query, so the summary has the final statistics. It encodes
	// the numbers as JSON strings.
	summary := struct {
		ReadRows  int64 `json:"read_rows,string"`
		ReadBytes int64 `json:"read_bytes,string"`
	}{}
	if err := json.Unmarshal([]byte(rsp.Header.Get("X-ClickHouse-Summary")), &summary); err != nil {
		return nil, status.InternalErrorf("parse ClickHouse summary: %s", err)
	}
	return &tqpb.QueryResponse{
		// ClickHouse strings can hold arbitrary bytes, but proto strings must
		// be valid UTF-8.
		Output:    strings.ToValidUTF8(string(body), string(utf8.RuneError)),
		ReadRows:  summary.ReadRows,
		ReadBytes: summary.ReadBytes,
	}, nil
}

// clickhouseError converts the response to a failed query into a status
// error. We keep the message from ClickHouse, because it tells the tenant
// what is wrong with the query.
func clickhouseError(code int, body string) error {
	msg := versionSuffixRE.ReplaceAllString(strings.TrimSpace(body), "")
	switch chproto.Error(code) {
	case 0:
		// ClickHouse sets an exception code on every error, so something
		// between us and ClickHouse sent this response.
		return status.UnavailableErrorf("query ClickHouse: %s", msg)
	case chproto.ErrAuthenticationFailed:
		return status.InternalErrorf("authenticate to ClickHouse: %s", msg)
	case chproto.ErrReadonly, chproto.ErrAccessDenied:
		return status.PermissionDeniedError(msg)
	case chproto.ErrQuotaExceeded, chproto.ErrTooManySimultaneousQueries, chproto.ErrMemoryLimitExceeded, chproto.ErrTooManyRows, chproto.ErrTooManyBytes, chproto.ErrTooManyRowsOrBytes:
		return status.ResourceExhaustedError(msg)
	case chproto.ErrTimeoutExceeded, chproto.ErrTooSlow:
		return status.DeadlineExceededError(msg)
	default:
		return status.InvalidArgumentError(msg)
	}
}
