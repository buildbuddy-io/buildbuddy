// Package kythestorage implements a graphstore.Service using a Pebble backend
// database.
package kythestorage

import (
	"bytes"
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"kythe.io/kythe/go/storage/keyvalue"

	"github.com/cockroachdb/pebble"
)

type pebbleDB struct {
	env environment.Env
	db  *pebble.DB
}

// OpenRaw returns a keyvalue DB backed by the provided pebble database.
func OpenRaw(env environment.Env, db *pebble.DB) keyvalue.DB {
	return &pebbleDB{env: env, db: db}
}

func (p *pebbleDB) groupID(ctx context.Context) ([]byte, error) {
	auth := p.env.GetAuthenticator()
	u, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	return []byte(u.GetGroupID() + "/"), nil
}

func addPrefix(key, prefix []byte) []byte {
	buf := make([]byte, len(key)+len(prefix))
	copy(buf, prefix)
	copy(buf[len(prefix):], key)
	return buf
}

func stripPrefix(key, prefix []byte) []byte {
	if !bytes.HasPrefix(key, prefix) {
		log.Warningf("key %q should have had prefix %q but did not!", key, prefix)
	}
	return bytes.TrimPrefix(key, prefix)
}

// Close will close the underlying pebble database.
func (p *pebbleDB) Close(_ context.Context) error {
	return p.db.Close()
}

func (p *pebbleDB) readerForOpts(opts *keyvalue.Options) pebble.Reader {
	if snap := opts.GetSnapshot(); snap != nil {
		return snap.(*pebble.Snapshot)
	}
	return p.db
}

func (p *pebbleDB) Get(ctx context.Context, key []byte, opts *keyvalue.Options) ([]byte, error) {
	r := p.readerForOpts(opts)
	gID, err := p.groupID(ctx)
	if err != nil {
		return nil, err
	}
	value, closer, err := r.Get(addPrefix(key, gID))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, io.EOF
		}
		return nil, err
	}
	defer closer.Close()
	return value, nil
}

func (p *pebbleDB) NewSnapshot(ctx context.Context) keyvalue.Snapshot {
	snap := p.db.NewSnapshot()
	return snap
}

// pebbleWriter wraps a pebble batch and makes it keyvalue.Writer compatible.
type pebbleWriter struct {
	gID []byte
	*pebble.Batch
}

func (pw *pebbleWriter) Write(key, val []byte) error {
	pw.Batch.Set(addPrefix(key, pw.gID), val, nil /*=ignored write options*/)
	return nil
}

func (pw *pebbleWriter) Close() error {
	defer pw.Batch.Close()
	if err := pw.Batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func (p *pebbleDB) Writer(ctx context.Context) (keyvalue.Writer, error) {
	gID, err := p.groupID(ctx)
	if err != nil {
		return nil, err
	}
	return &pebbleWriter{gID, p.db.NewBatch()}, nil
}

// pebbleIter wraps a pebble iterator and makes it keyvalue.Iterator compatible.
type pebbleIter struct {
	gID []byte
	*pebble.Iterator
}

func (pi *pebbleIter) Next() ([]byte, []byte, error) {
	if pi.Iterator.Valid() {
		key := make([]byte, len(pi.Iterator.Key()))
		val := make([]byte, len(pi.Iterator.Value()))
		copy(key, pi.Iterator.Key())
		copy(val, pi.Iterator.Value())
		pi.Iterator.Next()
		return stripPrefix(key, pi.gID), val, nil
	}
	return nil, nil, io.EOF
}

func (pi *pebbleIter) Seek(key []byte) error {
	if !pi.Iterator.SeekGE(addPrefix(key, pi.gID)) {
		return io.EOF
	}
	return nil
}

func (p *pebbleDB) ScanPrefix(ctx context.Context, prefix []byte, opts *keyvalue.Options) (keyvalue.Iterator, error) {
	r := p.readerForOpts(opts)
	gID, err := p.groupID(ctx)
	if err != nil {
		return nil, err
	}
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: gID,
		UpperBound: append(gID, 255),
	})
	if err != nil {
		return nil, err
	}
	if len(prefix) == 0 {
		iter.First()
	} else {
		iter.SeekGE(addPrefix(prefix, gID))
	}
	return &pebbleIter{gID, iter}, nil
}

func (p *pebbleDB) ScanRange(ctx context.Context, rng *keyvalue.Range, opts *keyvalue.Options) (keyvalue.Iterator, error) {
	r := p.readerForOpts(opts)
	gID, err := p.groupID(ctx)
	if err != nil {
		return nil, err
	}
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: rng.Start,
		UpperBound: rng.End,
	})
	if err != nil {
		return nil, err
	}
	iter.First()
	return &pebbleIter{gID, iter}, nil
}
