package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

// unusedConnector fails to connect. Reading stats never connects.
type unusedConnector struct{}

func (unusedConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("not implemented")
}
func (unusedConnector) Driver() driver.Driver { return nil }

func TestStatsRecorderPoll_ReturnsWhenStopped(t *testing.T) {
	sqlDB := sql.OpenDB(unusedConnector{})
	t.Cleanup(func() { sqlDB.Close() })
	r := &dbStatsRecorder{db: sqlDB, role: "test"}
	stop := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		r.poll(stop)
		close(returned)
	}()

	close(stop)

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("poll did not return after stop was closed")
	}
}
