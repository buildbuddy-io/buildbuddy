package server

import (
	"context"

	"kythe.io/kythe/go/services/filetree"
	"kythe.io/kythe/go/services/graph"
	"kythe.io/kythe/go/services/xrefs"
	"kythe.io/kythe/go/serving/identifiers"
	"kythe.io/kythe/go/storage/keyvalue"
	"kythe.io/kythe/go/storage/pebble"
	"kythe.io/kythe/go/storage/table"

	ftsrv "kythe.io/kythe/go/serving/filetree"
	gsrv "kythe.io/kythe/go/serving/graph"
	xsrv "kythe.io/kythe/go/serving/xrefs"
)

func New(rootDirectory string) (*kytheServer, error) {
	db, err := pebble.Open(rootDirectory, nil /*use default options*/)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	xs := xsrv.NewService(ctx, db)
	tbl := &table.KVProto{DB: db}
	gs := gsrv.NewCombinedTable(tbl)
	ft := &ftsrv.Table{Proto: tbl, PrefixedKeys: true}
	it := &identifiers.Table{Proto: tbl}

	return &kytheServer{
		db: db,
		xs: xs,
		gs: gs,
		it: it,
		ft: ft,
	}, nil
}

type kytheServer struct {
	db keyvalue.DB
	xs xrefs.Service
	gs graph.Service
	it identifiers.Service
	ft filetree.Service
}

func (ks *kytheServer) XrefsService() xrefs.Service {
	return ks.xs
}
func (ks *kytheServer) GraphService() graph.Service {
	return ks.gs
}
func (ks *kytheServer) IdentifierService() identifiers.Service {
	return ks.it
}
func (ks *kytheServer) FiletreeService() filetree.Service {
	return ks.ft
}

func (ks *kytheServer) Close(ctx context.Context) {
	ks.db.Close(ctx)
}
