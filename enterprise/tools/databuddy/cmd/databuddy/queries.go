package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	dbpb "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/proto/databuddy"
)

const (
	casDir          = "cas"
	resultsCacheDir = "results"
)

func ContentAddress(content string) string {
	hash := sha256.Sum256([]byte(content))
	hex := fmt.Sprintf("%x", hash[:])
	return filepath.Join("sha256", hex)
}

func LookupQuery(ctx context.Context, db QueryDB, store interfaces.Blobstore, queryID string) (*Query, string, error) {
	query, err := db.GetQueryByID(ctx, queryID)
	if err != nil {
		return nil, "", fmt.Errorf("get query by ID: %w", err)
	}

	blobName := filepath.Join(casDir, query.ContentDigest)
	b, err := store.ReadBlob(ctx, blobName)
	if err != nil {
		return nil, "", fmt.Errorf("read blob %q: %w", blobName, err)
	}

	return query, string(b), nil
}

func SaveQuery(ctx context.Context, db QueryDB, store interfaces.Blobstore, queryID, query string) error {
	// Write blob
	contentAddress := ContentAddress(query)
	blobName := filepath.Join(casDir, contentAddress)
	if _, err := store.WriteBlob(ctx, blobName, []byte(query)); err != nil {
		return fmt.Errorf("write blob %q: %w", blobName, err)
	}

	q := &Query{
		QueryID:       queryID,
		ContentDigest: contentAddress,
	}

	// Parse YAML comments to get metadata
	config, _ := ParseYAMLComments(query)
	if config != nil {
		q.Name = config.Name
	}

	// TODO: don't allow overwriting other users' queries
	q.Author = VouchUser(ctx)

	// Upsert
	if err := db.UpsertQuery(ctx, q); err != nil {
		return fmt.Errorf("save query to DB: %w", err)
	}

	return nil
}

func CacheQueryResult(ctx context.Context, store interfaces.Blobstore, queryID string, result *dbpb.QueryResult) error {
	b, err := proto.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal query result: %w", err)
	}

	blobName := filepath.Join(resultsCacheDir, queryID)
	if _, err := store.WriteBlob(ctx, blobName, b); err != nil {
		return fmt.Errorf("write blob %q: %w", blobName, err)
	}
	return nil
}

func GetCachedQueryResult(ctx context.Context, store interfaces.Blobstore, queryID string) (*dbpb.QueryResult, error) {
	blobName := filepath.Join(resultsCacheDir, queryID)
	b, err := store.ReadBlob(ctx, blobName)
	if err != nil {
		return nil, status.WrapErrorf(err, "read blob %q", blobName)
	}

	result := &dbpb.QueryResult{}
	if err := proto.Unmarshal(b, result); err != nil {
		return nil, status.InternalErrorf("unmarshal result: %s", err)
	}

	return result, nil
}

var macroRegexp = regexp.MustCompile(`\$[A-Za-z_0-9]+`)

// ResolveMacros looks for strings like $Foo and replaces them with the contents
// of the query named 'Foo'.
func ResolveMacros(ctx context.Context, qdb QueryDB, store interfaces.Blobstore, queryContent string) (string, error) {
	macros := macroRegexp.FindAllString(queryContent, -1)
	macroSet := map[string]bool{}
	for _, m := range macros {
		macroSet[m] = true
	}

	type result struct{ Content string }
	resolved := map[string]*result{}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(8)
	for macro := range macroSet {
		res := &result{}
		resolved[macro] = res
		eg.Go(func() error {
			name := strings.TrimPrefix(macro, "$")
			q, err := qdb.GetQueryByName(ctx, name)
			if err != nil {
				if db.IsRecordNotFound(err) {
					return status.NotFoundErrorf("resolve macro %q: query with name %q not found", macro, name)
				}
				return err
			}
			blobName := filepath.Join(casDir, q.ContentDigest)
			b, err := store.ReadBlob(ctx, blobName)
			if err != nil {
				return fmt.Errorf("read blob %q: %w", blobName, err)
			}
			// TODO: resolve macros recursively.
			res.Content = string(b)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return "", err
	}
	return macroRegexp.ReplaceAllStringFunc(queryContent, func(m string) string {
		return "\n" + resolved[m].Content + "\n"
	}), nil
}

var disallowedRegexp = regexp.MustCompile(`(?i)\b(insert|update|delete|merge|create|alter|drop|truncate|grant|revoke)\b`)

func ValidateSQL(query string) error {
	lines := strings.Split(query, "\n")
	for i, line := range lines {
		// TODO: parse SQL AST.
		matchRange := disallowedRegexp.FindStringIndex(line)
		if len(matchRange) == 0 {
			continue
		}
		start, end := matchRange[0], matchRange[1]
		before := query[:start]
		match := query[start:end]
		after := query[end:]
		if len(before) > 10 {
			before = "..." + before[len(before)-10:]
		}
		if len(after) > 10 {
			after = after[:10] + "..."
		}
		return fmt.Errorf("operation not permitted: line %d, column %d: %q", i+1, start+1, before+"[["+after+"]]"+match)
	}
	return nil
}

func QueryToProto(q *Query) *dbpb.QueryMetadata {
	return &dbpb.QueryMetadata{
		QueryId:    q.QueryID,
		ModifiedAt: q.Model.UpdatedAtUsec,
		Name:       q.Name,
		Author:     q.Author,
	}
}
