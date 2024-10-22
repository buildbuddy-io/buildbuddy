//go:build !cgo

package db

import (
	"database/sql/driver"
	"errors"
	"fmt"
	spannerDriverLib "github.com/googleapis/go-sql-spanner"

	gomysql "github.com/go-sql-driver/mysql"
	gopostgres "github.com/jackc/pgx/v5/stdlib"
)

func getDriver(ds DataSource) (driver.Driver, error) {
	switch ds.DriverName() {
	case mysqlDriver:
		return &gomysql.MySQLDriver{}, nil
	case postgresDriver:
		return &gopostgres.Driver{}, nil
	case spannerDriver:
		return spannerDriverLib.Driver{}, nil
	default:
		return nil, fmt.Errorf("unsupported database driver %s", ds.DriverName())
	}
}

func (h *DBHandle) IsDuplicateKeyError(err error) bool {
	var mysqlErr *gomysql.MySQLError
	// Defined at https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_dup_entry
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}
	return false
}
