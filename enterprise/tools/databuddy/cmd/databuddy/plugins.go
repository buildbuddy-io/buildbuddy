package main

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"

	dbpb "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/proto/databuddy"
)

type QueryProcessor interface {
	ProcessQuery(ctx context.Context, query string) (string, error)
}

type ResultsProcessor interface {
	ProcessResults(ctx context.Context, res *dbpb.QueryResult) error
}

type Plugin interface {
	Name() string
}

type groupSlugPlugin struct {
	oltpDatasource *DataSource

	singleflight singleflight.Group[string, any]
	refreshAt    time.Time
	slugsByID    map[string]string
	idsBySlug    map[string]string
}

var _ QueryProcessor = (*groupSlugPlugin)(nil)
var _ ResultsProcessor = (*groupSlugPlugin)(nil)

func NewGroupSlugPlugin(datasources []*DataSource) (Plugin, error) {
	for _, ds := range datasources {
		if ds.ID() == "oltp" {
			return &groupSlugPlugin{
				oltpDatasource: ds,
			}, nil
		}
	}
	return nil, fmt.Errorf("missing 'oltp' datasource")
}

func (g *groupSlugPlugin) Name() string {
	return "group_slug"
}

func (g *groupSlugPlugin) refreshGroups(ctx context.Context) error {
	_, _, err := g.singleflight.Do(ctx, "", func(ctx context.Context) (any, error) {
		if time.Now().Before(g.refreshAt) {
			return nil, nil
		}
		g.slugsByID = map[string]string{}
		g.idsBySlug = map[string]string{}
		rows, err := g.oltpDatasource.GORM(ctx).Raw(`
			SELECT
				group_id,
				url_identifier
			FROM
				` + "`" + `Groups` + "`" + `
		`).Rows()
		if err != nil {
			return nil, fmt.Errorf("query groups: %w", err)
		}
		for rows.Next() {
			var groupID, slug *string
			if err := rows.Scan(&groupID, &slug); err != nil {
				return nil, fmt.Errorf("scan group row: %w", err)
			}
			if groupID != nil && slug != nil {
				g.slugsByID[*groupID] = *slug
				g.idsBySlug[*slug] = *groupID
			}
		}
		if rows.Err() != nil {
			return nil, fmt.Errorf("scan group rows: %w", err)
		}
		g.refreshAt = time.Now().Add(15 * time.Minute)
		return nil, nil
	})
	return err
}

var (
	lookupRegexp = regexp.MustCompile(`groupLookup\s*\(\s*("(.*?)"|'(.*?)')\s*\)`)
	stringRegexp = regexp.MustCompile(`("(.*?)"|'(.*?)')`)
)

func (g *groupSlugPlugin) ProcessQuery(ctx context.Context, query string) (string, error) {
	if err := g.refreshGroups(ctx); err != nil {
		return "", fmt.Errorf("refresh groups: %w", err)
	}
	var err error
	query = lookupRegexp.ReplaceAllStringFunc(query, func(s string) string {
		m := stringRegexp.FindStringSubmatch(s)
		if len(m) != 4 {
			err = fmt.Errorf("failed to parse string expression from %q (match=%v)", s, m)
			return ""
		}
		var slug string
		if m[2] != "" {
			slug = m[2]
		} else {
			slug = m[3]
		}
		groupID := g.idsBySlug[slug]
		if groupID == "" {
			err = fmt.Errorf("no such group %q", s)
			return ""
		}
		return `'` + groupID + `'`
	})
	if err != nil {
		return "", err
	}
	return query, nil
}

func (g *groupSlugPlugin) ProcessResults(ctx context.Context, res *dbpb.QueryResult) error {
	columnIndex := -1
	for i, c := range res.Columns {
		if c.Name == "group_id" {
			columnIndex = i
			break
		}
	}
	if columnIndex < 0 {
		return nil
	}
	if err := g.refreshGroups(ctx); err != nil {
		return fmt.Errorf("refresh groups: %w", err)
	}
	groupIDs := res.Columns[columnIndex].GetData().GetStringValues()
	slugs := make([]string, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		slugs = append(slugs, g.slugsByID[groupID])
	}
	groupSlugColumn := &dbpb.Column{
		Name:             "group_slug",
		DatabaseTypeName: "String",
		Data: &dbpb.ColumnData{
			StringValues: slugs,
		},
	}
	res.Columns = append(
		res.Columns[:columnIndex+1],
		append([]*dbpb.Column{groupSlugColumn}, res.Columns[columnIndex+1:]...)...)
	return nil
}
