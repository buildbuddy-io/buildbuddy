package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"gorm.io/gorm"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dbpb "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/proto/databuddy"
	gormclickhouse "gorm.io/driver/clickhouse"
)

type DataSourceConfig struct {
	ID  string `json:"id" yaml:"id"`
	DSN string `json:"dsn" yaml:"dsn"`
}

type DataSource struct {
	config DataSourceConfig
	db     *gorm.DB
}

func NewDataSource(ctx context.Context, cfg DataSourceConfig) (*DataSource, error) {
	if strings.HasPrefix(cfg.DSN, "clickhouse://") {
		return newClickHouseDataSource(ctx, cfg.DSN)
	} else {
		dbh, _, err := db.Open(ctx, cfg.DSN, &db.AdvancedConfig{})
		if err != nil {
			return nil, err
		}
		return &DataSource{
			config: cfg,
			db:     dbh,
		}, nil
	}
}

func newClickHouseDataSource(ctx context.Context, dsn string) (*DataSource, error) {
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}

	sqlDB := clickhouse.OpenDB(options)
	db, err := gorm.Open(gormclickhouse.New(gormclickhouse.Config{
		Conn:                         sqlDB,
		DontSupportEmptyDefaultValue: true,
	}))
	if err != nil {
		return nil, fmt.Errorf("open DB: %w", err)
	}
	return &DataSource{db: db}, nil
}

func (ds *DataSource) ID() string {
	return ds.config.ID
}

func (ds *DataSource) GORM(ctx context.Context) *gorm.DB {
	return ds.db.WithContext(ctx)
}

func (ds *DataSource) Query(ctx context.Context, query string) (*dbpb.QueryResult, error) {
	rows, err := ds.db.WithContext(ctx).Raw(query).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columnData := make([][]any, len(columnTypes))

	for rows.Next() {
		rowData := make([]any, len(columnTypes))
		rowPointers := make([]any, len(columnTypes))
		for i := range rowData {
			rowPointers[i] = &rowData[i]
		}

		if err := rows.Scan(rowPointers...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		for i, data := range rowData {
			columnData[i] = append(columnData[i], data)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan rows: %w", err)
	}

	columns := make([]*dbpb.Column, len(columnTypes))
	for i, c := range columnTypes {
		data := columnData[i]
		columnData, err := getColumnData(c.ScanType().String(), data)
		if err != nil {
			log.Warningf("Failed to scan column %q: %s", c.Name(), err)
		}
		columns[i] = &dbpb.Column{
			Name:             c.Name(),
			DatabaseTypeName: c.DatabaseTypeName(),
			Data:             columnData,
		}
	}

	queryResult := &dbpb.QueryResult{
		Columns: columns,
	}

	return queryResult, nil
}

func getColumnData(scanType string, columnData []any) (*dbpb.ColumnData, error) {
	d := &dbpb.ColumnData{}
	for i, v := range columnData {
		if v == nil {
			d.NullIndexes = append(d.NullIndexes, int32(i))
			continue
		}
		switch scanType {
		case "string":
			d.StringValues = append(d.StringValues, v.(string))
		case "int8":
			d.Int32Values = append(d.Int32Values, int32(v.(int8)))
		case "int32":
			d.Int32Values = append(d.Int32Values, v.(int32))
		case "int64":
			d.Int64Values = append(d.Int64Values, v.(int64))
		case "uint8":
			d.Uint32Values = append(d.Uint32Values, uint32(v.(uint8)))
		case "float64":
			d.DoubleValues = append(d.DoubleValues, v.(float64))
		case "time.Time":
			// Format timestamps as ISO strings for now.
			d.StringValues = append(d.StringValues, v.(time.Time).Format("2006-01-02T15:04:05.000000Z"))
		case "[]string":
			d.ArrayValues = append(d.ArrayValues, &dbpb.ColumnData{
				StringValues: v.([]string),
			})
		// TODO: handle arbitrary array types (probably need reflection?)
		default:
			return nil, fmt.Errorf("unimplemented scan type %q", scanType)
		}
	}
	return d, nil
}

func stripURLCredentials(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	if u.User != nil {
		u.User = nil
	}
	return u.String()
}
