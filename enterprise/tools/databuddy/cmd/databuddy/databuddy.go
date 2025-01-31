package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/bundle"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	dbpb "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/proto/databuddy"

	_ "embed"
)

var (
	listenAddr = flag.String("databuddy.http.listen", ":8083", "TCP address to listen on")
	// TODO: multiple data sources, with some id for each (so we can choose
	// between datasources in the UI).
	dataSources = flag.Slice("databuddy.data_sources", []DataSourceConfig{}, "Data sources")
)

//go:embed static/index.html
var indexHTML []byte

//go:embed static/styles.css
var stylesCSS []byte

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx := context.Background()

	if err := configsecrets.Configure(); err != nil {
		return fmt.Errorf("configure config secrets: %w", err)
	}
	if err := config.Load(); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := log.Configure(); err != nil {
		return fmt.Errorf("configure logging: %w", err)
	}

	oltpDSN, err := flagutil.GetDereferencedValue[string]("database.data_source")
	if err != nil {
		return fmt.Errorf("lookup 'database.data_source' config: %w", err)
	}

	if len(*dataSources) == 0 {
		// Default data source config
		*dataSources = []DataSourceConfig{
			{ID: "olap", DSN: "clickhouse://localhost:9000/buildbuddy_local"},
		}
		if strings.HasPrefix(oltpDSN, "sqlite3://") {
			oltp := DataSourceConfig{ID: "oltp", DSN: oltpDSN}
			*dataSources = append(*dataSources, oltp)
		}
	}

	var datasources []*DataSource
	for _, cfg := range *dataSources {
		ds, err := NewDataSource(ctx, cfg)
		if err != nil {
			return fmt.Errorf("init data source %q: %w", cfg.ID, err)
		}
		log.Infof("Configured data source %q with DSN %q", cfg.ID, stripURLCredentials(cfg.DSN))
		datasources = append(datasources, ds)
	}

	gormDB, _, err := db.Open(ctx, oltpDSN, &db.AdvancedConfig{})
	if err != nil {
		return fmt.Errorf("open OLTP db: %w", err)
	}

	log.Infof("Starting OLTP database migrations")
	if err := gormDB.AutoMigrate(&Query{}); err != nil {
		return fmt.Errorf("migrate DB: %w", err)
	}
	log.Infof("Completed OLTP database migrations")

	queryDB := NewQueryDB(gormDB)

	log.Infof("Configured OLTP DB %q", stripURLCredentials(oltpDSN))

	blobstore, err := blobstore.NewFromConfig(ctx)
	if err != nil {
		return fmt.Errorf("init blobstore from configuration: %w", err)
	}

	gs, err := NewGroupSlugPlugin(datasources)
	if err != nil {
		return fmt.Errorf("init group slug plugin: %w", err)
	}
	plugins := []Plugin{gs}

	server := &Server{
		db:          queryDB,
		blobstore:   blobstore,
		datasources: datasources,
		plugins:     plugins,
	}

	hc := healthcheck.NewHealthChecker("databuddy")

	grpcServer := grpc.NewServer()
	dbpb.RegisterQueriesServer(grpcServer, server)
	rpcHandler, err := protolet.GenerateHTTPHandlers("/rpc/io.buildbuddy.databuddy.Queries/", "io.buildbuddy.databuddy.Queries", server, grpcServer)
	if err != nil {
		return fmt.Errorf("init protolet handlers: %w", err)
	}

	mux := http.NewServeMux()

	mux.Handle("/styles.css", SetContentType(ServeContentWithETagCaching(bytes.NewReader(stylesCSS)), "text/css"))
	mux.Handle("/index.js", SetContentType(ServeContentWithETagCaching(bytes.NewReader(bundle.IndexJS)), "application/javascript"))
	mux.Handle("/monaco/vs-theme.css", SetContentType(ServeContentWithETagCaching(bytes.NewReader(bundle.MonacoCSS)), "text/css"))
	mux.Handle("/monaco/editor.worker.js", SetContentType(ServeContentWithETagCaching(bytes.NewReader(bundle.MonacoWorkerJS)), "application/javascript"))
	mux.Handle("/uplot/uplot.min.css", SetContentType(ServeContentWithETagCaching(bytes.NewReader(bundle.UPlotCSS)), "text/css"))

	mux.Handle("/", WithVouchAuth(http.HandlerFunc(server.ServeIndex)))
	mux.Handle("/rpc/io.buildbuddy.databuddy.Queries/", WithVouchAuth(rpcHandler.BodyParserMiddleware(rpcHandler.RequestHandler)))

	mux.Handle("/healthz", hc.LivenessHandler())
	mux.Handle("/readyz", hc.ReadinessHandler())

	// TODO: pprof, metrics

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", *listenAddr, err)
	}
	hc.RegisterShutdownFunction(func(ctx context.Context) error {
		return lis.Close()
	})

	log.Printf("Listening on %s", *listenAddr)
	if err := http.Serve(lis, LogServerErrors(mux)); err != nil {
		return fmt.Errorf("serve: %w", err)
	}

	return nil
}

type Server struct {
	db        QueryDB
	blobstore interfaces.Blobstore

	datasources []*DataSource
	plugins     []Plugin
}

var _ dbpb.QueriesServer = (*Server)(nil)

func (s *Server) ServeIndex(w http.ResponseWriter, r *http.Request) {
	html := string(indexHTML)
	initialDataJSON, err := protojson.Marshal(&dbpb.InitialData{
		User: VouchUser(r.Context()),
	})
	if err != nil {
		log.Errorf("Failed to marshal initial data: %s", err)
		initialDataJSON = []byte("{}")
	}
	html = strings.ReplaceAll(html, `<script id="initial-data"></script>`, `<script>window._initialData = `+string(initialDataJSON)+`;</script>`)
	io.WriteString(w, html)
}

func (s *Server) GetQueries(ctx context.Context, req *dbpb.GetQueriesRequest) (*dbpb.GetQueriesResponse, error) {
	rows, err := s.db.GetQueries(ctx)
	if err != nil {
		return nil, err
	}

	queries := make([]*dbpb.QueryMetadata, 0, len(rows))
	for _, row := range rows {
		queries = append(queries, QueryToProto(&row))
	}

	return &dbpb.GetQueriesResponse{Queries: queries}, nil
}

func (s *Server) GetQuery(ctx context.Context, req *dbpb.GetQueryRequest) (*dbpb.GetQueryResponse, error) {
	if req.GetQueryId() == "" {
		return nil, status.InvalidArgumentError("missing query ID")
	}
	q, sql, err := LookupQuery(ctx, s.db, s.blobstore, req.GetQueryId())
	if err != nil {
		return nil, err
	}

	md := QueryToProto(q)

	var charts []*dbpb.Chart
	var parseError string
	config, err := ParseYAMLComments(sql)
	if err != nil {
		parseError = err.Error()
	} else {
		c, err := ParseCharts(config)
		if err != nil {
			parseError = fmt.Errorf("parse charts section: %w", err).Error()
		}
		charts = c
	}

	// TODO: do this in parallel with DB lookup
	cachedResult, err := GetCachedQueryResult(ctx, s.blobstore, req.GetQueryId())
	if err != nil && !status.IsNotFoundError(err) {
		log.Printf("Failed to read cached query %q: %s", req.GetQueryId(), err)
	}

	res := &dbpb.GetQueryResponse{
		Metadata:     md,
		Sql:          sql,
		CachedResult: cachedResult,
		Charts:       charts,
		ParseError:   parseError,
	}
	return res, nil
}

func (s *Server) SaveQuery(ctx context.Context, req *dbpb.SaveQueryRequest) (*dbpb.SaveQueryResponse, error) {
	if req.GetQueryId() == "" {
		return nil, status.InvalidArgumentError("missing query ID")
	}

	sql := req.GetContent()
	if err := SaveQuery(ctx, s.db, s.blobstore, req.GetQueryId(), sql); err != nil {
		return nil, err
	}

	var charts []*dbpb.Chart
	var parseError string
	config, err := ParseYAMLComments(sql)
	if err != nil {
		parseError = err.Error()
	} else {
		// TODO: let client parse the charts?
		c, err := ParseCharts(config)
		if err != nil {
			parseError = fmt.Errorf("parse charts section: %w", err).Error()
		} else {
			charts = c
		}
	}

	return &dbpb.SaveQueryResponse{
		Charts:     charts,
		ParseError: parseError,
	}, nil
}

func (s *Server) ExecuteQuery(ctx context.Context, req *dbpb.ExecuteQueryRequest) (*dbpb.ExecuteQueryResponse, error) {
	if req.GetQueryId() == "" {
		return nil, status.InvalidArgumentError("missing query ID")
	}
	query := req.GetContent()
	if err := SaveQuery(ctx, s.db, s.blobstore, req.GetQueryId(), query); err != nil {
		return nil, err
	}

	query, err := ResolveMacros(ctx, s.db, s.blobstore, query)
	if err != nil {
		return nil, err
	}

	var charts []*dbpb.Chart
	var parseError string
	config, err := ParseYAMLComments(query)
	if err != nil {
		parseError = err.Error()
	} else {
		// TODO: let client parse the charts?
		c, err := ParseCharts(config)
		if err != nil {
			parseError = fmt.Errorf("parse charts section: %w", err).Error()
		} else {
			charts = c
		}
	}

	if config != nil && config.Macro {
		return nil, status.InvalidArgumentError("macros cannot be executed")
	}

	queryPlugins := s.pluginsForQuery(config)
	for _, plugin := range queryPlugins {
		if proc, ok := plugin.(QueryProcessor); ok {
			q, err := proc.ProcessQuery(ctx, query)
			if err != nil {
				return nil, fmt.Errorf("%s plugin error: %w", plugin.Name(), err)
			}
			query = q
		}
	}

	// TODO: rely on users/permissions to prevent mutates, not parsing.
	if err := ValidateSQL(query); err != nil {
		return nil, status.InvalidArgumentErrorf("invalid query: %s", err)
	}

	start := time.Now()
	log.Infof("Running query %q", query)
	res, err := s.datasources[0].Query(ctx, query)
	if err != nil {
		// TODO: Query() should probably do this
		res = &dbpb.QueryResult{
			Error: err.Error(),
		}
	}
	log.Infof("Query completed in %s", time.Since(start))

	for _, plugin := range queryPlugins {
		if proc, ok := plugin.(ResultsProcessor); ok {
			if err := proc.ProcessResults(ctx, res); err != nil {
				return nil, fmt.Errorf("%s plugin error: %w", plugin.Name(), err)
			}
		}
	}

	if err := CacheQueryResult(ctx, s.blobstore, req.GetQueryId(), res); err != nil {
		log.Printf("Failed to cache result: %s", err)
	}

	return &dbpb.ExecuteQueryResponse{
		Result:     res,
		Charts:     charts,
		ParseError: parseError,
	}, nil
}

func (s *Server) GetSchema(ctx context.Context, req *dbpb.GetSchemaRequest) (*dbpb.GetSchemaResponse, error) {
	// TODO: this query is prob clickhouse specific
	rows, err := s.datasources[0].GORM(ctx).Raw(`
		SELECT
			table,
			name
		FROM
			system.columns
		WHERE database = currentDatabase()
		ORDER BY table, position
	`).Rows()
	if err != nil {
		return nil, err
	}
	cols := map[string][]string{}
	for rows.Next() {
		var tableName, columnName string
		if err := rows.Scan(&tableName, &columnName); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		cols[tableName] = append(cols[tableName], columnName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan rows: %w", err)
	}
	res := &dbpb.GetSchemaResponse{}
	for tableName, columnNames := range cols {
		tbl := &dbpb.TableSchema{Name: tableName}
		for _, c := range columnNames {
			tbl.Columns = append(tbl.Columns, &dbpb.ColumnSchema{Name: c})
		}
		res.Tables = append(res.Tables, tbl)
	}
	return res, nil
}

func (s *Server) pluginsForQuery(cfg *QueryConfig) []Plugin {
	var out []Plugin
	for _, name := range cfg.Plugins {
		for _, p := range s.plugins {
			if p.Name() == name {
				out = append(out, p)
			}
		}
	}
	return out
}
