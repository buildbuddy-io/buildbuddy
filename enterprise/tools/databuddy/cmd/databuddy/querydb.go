package main

import (
	"context"

	"gorm.io/gorm"
)

type Model struct {
	CreatedAtUsec int64
	UpdatedAtUsec int64
}

func (m *Model) BeforeCreate(tx *gorm.DB) (err error) {
	nowUsec := tx.Config.NowFunc().UnixMicro()
	m.CreatedAtUsec = nowUsec
	m.UpdatedAtUsec = nowUsec
	return nil
}

func (m *Model) BeforeUpdate(tx *gorm.DB) (err error) {
	m.UpdatedAtUsec = tx.Config.NowFunc().UnixMicro()
	return nil
}

type Query struct {
	Model

	QueryID string `gorm:"query_id;primaryKey"`
	Name    string `gorm:"name;index:name_index;unique"`
	Author  string `gorm:"author"`
	// ContentDigest identifies SQL contents using a CAS ID format:
	// "{HASH_FUNCTION}/{HASH}"
	ContentDigest string `gorm:"content_digest"`
}

func (*Query) TableName() string {
	return "Queries"
}

// TODO: query version history, including cached results for each query

type QueryDB interface {
	GetQueries(ctx context.Context) ([]Query, error)
	GetQueryByID(ctx context.Context, queryID string) (*Query, error)
	GetQueryByName(ctx context.Context, name string) (*Query, error)
	UpsertQuery(ctx context.Context, query *Query) error
}

type queryDB struct {
	db *gorm.DB
}

var _ QueryDB = (*queryDB)(nil)

func NewQueryDB(db *gorm.DB) QueryDB {
	return &queryDB{db: db}
}

func (q *queryDB) GetQueries(ctx context.Context) ([]Query, error) {
	var queries []Query
	if err := q.db.WithContext(ctx).Find(&queries).Error; err != nil {
		return nil, err
	}
	return queries, nil
}

func (q *queryDB) GetQueryByID(ctx context.Context, queryID string) (*Query, error) {
	var query Query
	if err := q.db.WithContext(ctx).First(&query, "query_id = ?", queryID).Error; err != nil {
		return nil, err
	}
	return &query, nil
}

func (q *queryDB) GetQueryByName(ctx context.Context, name string) (*Query, error) {
	var query Query
	if err := q.db.WithContext(ctx).First(&query, "name = ?", name).Error; err != nil {
		return nil, err
	}
	return &query, nil
}

func (q *queryDB) UpsertQuery(ctx context.Context, query *Query) error {
	return q.db.WithContext(ctx).Save(query).Error
}
