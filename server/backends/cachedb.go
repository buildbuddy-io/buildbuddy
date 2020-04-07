package cachedb

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/jinzhu/gorm"
)

type CacheDB struct {
	h *db.DBHandle
}

func NewCacheDB(h *db.DBHandle) *CacheDB {
	return &CacheDB{h: h}
}

func (d *CacheDB) InsertOrUpdateCacheEntry(ctx context.Context, c *tables.CacheEntry) error {
	return d.h.Transaction(func(tx *gorm.DB) error {
		var existing tables.CacheEntry
		if err := tx.Where("entry_id = ?", c.EntryID).First(&existing).Error; err != nil {
			if gorm.IsRecordNotFoundError(err) {
				return tx.Create(c).Error
			}
			return err
		}
		return tx.Model(&existing).Where("entry_id = ?", c.EntryID).Updates(c).Error
	})
}

func (d *CacheDB) IncrementEntryReadCount(ctx context.Context, key string) error {
	return d.h.Exec(`UPDATE CacheEntries SET read_count = read_count + 1
                              WHERE entry_id = ?`, key).Error
}

func (d *CacheDB) DeleteCacheEntry(ctx context.Context, key string) error {
	c := &tables.CacheEntry{EntryID: key}
	return d.h.Delete(c).Error
}

func (d *CacheDB) SumCacheEntrySizes(ctx context.Context) (int64, error) {
	result := struct {
		// NB: field name must be camelcase because gorm will
		// de-c-stringify the sql field name to determine this one, and
		// if it does not find a matching field it will silently drop
		// the data.
		TotalSizeBytes int64
	}{}
	qr := d.h.Raw(`SELECT SUM(ce.size_bytes) as total_size_bytes FROM CacheEntries as ce`)
	if err := qr.Scan(&result).Error; err != nil {
		return 0, err
	}
	return result.TotalSizeBytes, nil
}

func (d *CacheDB) RawQueryCacheEntries(ctx context.Context, sql string, values ...interface{}) ([]*tables.CacheEntry, error) {
	rows, err := d.h.Raw(sql, values...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]*tables.CacheEntry, 0)
	var ce tables.CacheEntry
	for rows.Next() {
		if err := d.h.ScanRows(rows, &ce); err != nil {
			return nil, err
		}
		i := ce
		entries = append(entries, &i)
	}
	return entries, nil
}
