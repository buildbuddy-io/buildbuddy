package featureflag

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	ffpb "github.com/buildbuddy-io/buildbuddy/proto/featureflag"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"strings"
	"sync"
	"time"
)

var (
	cacheTTL = flag.Duration("featureflag.cache_ttl", 5*time.Minute, "Duration of time feature flags will be cached in memory.")
)

const (
	// The number of feature flags that we will cache in memory.
	flagCacheSize = 1000

	// The number ofexperiment assignments we will cache in memory.
	experimentAssignmentCacheSize = 100_000
)

type FeatureFlagService struct {
	env              environment.Env
	featureFlagCache *featureFlagCache
}

func NewFeatureFlagService(env environment.Env) (*FeatureFlagService, error) {
	cache, err := newCache()
	if err != nil {
		return nil, err
	}
	ff := &FeatureFlagService{
		env:              env,
		featureFlagCache: cache,
	}
	return ff, nil
}

type FeatureFlag struct {
	Name        string
	Enabled     bool
	Description string
	// If Enabled=true and this is set, the list of group IDs the feature flag applies to
	// If Enabled=true and this is not set, the feature flag is enabled for all groups
	// If Enabled=false and this is set, the feature flag will still be disabled for all groups.
	AssignedGroupIDs []string
}

// featureFlagAssignment is a temporary struct used when joining featureflag related tables
type featureFlagAssignment struct {
	name        string
	enabled     bool
	description string
	groupID     *string
}

func (ffs *FeatureFlagService) GetAll(ctx context.Context) ([]*ffpb.FeatureFlag, error) {
	fmt.Println("GetAll flags")
	rq := ffs.env.GetDBHandle().NewQuery(ctx, "feature_flag_service_get_all").Raw(
		`SELECT ff.name, enabled, description, group_id FROM "FeatureFlags" ff LEFT JOIN "ExperimentAssignments" ea on ff.name = ea.name ORDER BY ff.name;`,
	)
	return ffs.parseFlags(rq)
}

func (ffs *FeatureFlagService) FetchFlag(ctx context.Context, name string) (*ffpb.FeatureFlag, error) {
	fmt.Println("Fetch Flag")
	rq := ffs.env.GetDBHandle().NewQuery(ctx, "feature_flag_service_get_all").Raw(
		`SELECT ff.name, enabled, description, group_id FROM "FeatureFlags" ff LEFT JOIN "ExperimentAssignments" ea on ff.name = ea.name WHERE ff.name = ?;`,
		name,
	)
	flags, err := ffs.parseFlags(rq)
	if err != nil {
		return nil, err
	}

	if len(flags) == 0 {
		return nil, status.NotFoundErrorf("flag %s not found", name)
	}
	return flags[0], nil
}

func (ffs *FeatureFlagService) parseFlags(query interfaces.DBRawQuery) ([]*ffpb.FeatureFlag, error) {
	assignments := make([]*featureFlagAssignment, 0)
	err := query.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
		a := &featureFlagAssignment{}
		if err := row.Scan(
			&a.name,
			&a.enabled,
			&a.description,
			&a.groupID,
		); err != nil {
			return status.WrapError(err, "parse feature flag join")
		}
		assignments = append(assignments, a)
		return nil
	})
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("no feature flags found")
		}
		return nil, status.InternalError(err.Error())
	}
	if len(assignments) == 0 {
		return nil, status.NotFoundError("no feature flags found")
	}

	// TODO: Make ExperimentGroupIds a map
	ffMap := make(map[string]*ffpb.FeatureFlag, 0)
	for _, assignment := range assignments {
		var ff *ffpb.FeatureFlag
		var exists bool
		if ff, exists = ffMap[assignment.name]; !exists {
			ff = &ffpb.FeatureFlag{
				Name:               assignment.name,
				Enabled:            assignment.enabled,
				Description:        assignment.description,
				ExperimentGroupIds: []string{},
			}
			ffMap[assignment.name] = ff
		}
		if assignment.groupID != nil && *assignment.groupID != "" {
			ff.ExperimentGroupIds = append(ff.ExperimentGroupIds, *assignment.groupID)
		}
	}

	ffSlice := make([]*ffpb.FeatureFlag, 0, len(ffMap))
	for _, ff := range ffMap {
		ffSlice = append(ffSlice, ff)
	}

	return ffSlice, nil
}

func (ffs *FeatureFlagService) CreateFeatureFlag(ctx context.Context, req *ffpb.CreateFeatureFlagRequest) (*ffpb.CreateFeatureFlagResponse, error) {
	if req.GetName() == "" {
		return nil, status.InvalidArgumentError("A name is required to create a new workflow.")
	}

	err := ffs.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		ff := &tables.FeatureFlag{
			Name:        req.GetName(),
			Description: req.GetDescription(),
			Enabled:     false,
		}
		return tx.NewQuery(ctx, "featureflag_service_insert_featureflag").Create(ff)
	})
	if err != nil {
		return nil, err
	}
	return &ffpb.CreateFeatureFlagResponse{}, nil
}

func (ffs *FeatureFlagService) UpdateFeatureFlag(ctx context.Context, req *ffpb.UpdateFeatureFlagRequest) (*ffpb.UpdateFeatureFlagResponse, error) {
	_ = ffs.featureFlagCache.UpdateFlag(req.Name, req.Enabled)
	if err := ffs.env.GetDBHandle().NewQuery(ctx, "featureflag_service_update_featureflag").Raw(`
				UPDATE "FeatureFlags"
				SET enabled = ?, description = ?
				WHERE name = ?`,
		req.Enabled, req.Description, req.Name,
	).Exec().Error; err != nil {
		return nil, status.WrapError(err, "update featureflag")
	}

	return &ffpb.UpdateFeatureFlagResponse{}, nil
}

func (ffs *FeatureFlagService) UpdateExperimentAssignments(ctx context.Context, req *ffpb.UpdateExperimentAssignmentsRequest) (*ffpb.UpdateExperimentAssignmentsResponse, error) {
	err := ffs.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		_ = ffs.featureFlagCache.UpdateExperimentAssignments(req.Name, req.GetConfiguredGroupIds())

		if len(req.GetConfiguredGroupIds()) == 0 {
			if err := tx.NewQuery(ctx, "featureflag_service_delete_group_featureflag").Raw(`
				DELETE FROM "ExperimentAssignments" WHERE name = ?`,
				req.Name,
			).Exec().Error; err != nil {
				return status.WrapError(err, "delete experiment assignments")
			}
		} else {
			if err := tx.NewQuery(ctx, "featureflag_service_delete_group_featureflag").Raw(`
				DELETE FROM "ExperimentAssignments"
				WHERE name = ? AND group_id NOT IN ?`,
				req.Name, req.GetConfiguredGroupIds(),
			).Exec().Error; err != nil {
				return status.WrapError(err, "delete experiment assignments")
			}
		}

		if len(req.GetConfiguredGroupIds()) > 0 {
			valueStrArr := make([]string, len(req.GetConfiguredGroupIds()))
			valueArr := make([]interface{}, len(req.GetConfiguredGroupIds())*2)
			for i, groupID := range req.GetConfiguredGroupIds() {
				valueStrArr[i] = "(?, ?)"
				valueArr[i*2] = req.Name
				valueArr[i*2+1] = groupID
			}
			valueStr := strings.Join(valueStrArr, ", ")

			if err := tx.NewQuery(ctx, "featureflag_service_insert_group_featureflag").Raw(`
				INSERT OR IGNORE INTO "ExperimentAssignments"
				(name, group_id) VALUES `+valueStr,
				valueArr...,
			).Exec().Error; err != nil {
				return status.WrapError(err, "update experiment assignments")
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return &ffpb.UpdateExperimentAssignmentsResponse{}, nil
}

func (ffs *FeatureFlagService) GetGroups(ctx context.Context) ([]*ffpb.Group, error) {
	rq := ffs.env.GetDBHandle().NewQuery(ctx, "feature_flag_service_get_groups").Raw(
		`SELECT group_id, name FROM "Groups"`,
	)
	groups := make([]*ffpb.Group, 0)
	err := rq.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
		g := &ffpb.Group{}
		if err := row.Scan(
			&g.GroupId,
			&g.Name,
		); err != nil {
			return status.WrapError(err, "parse group")
		}
		groups = append(groups, g)
		return nil
	})
	if err != nil {
		return nil, status.InternalError(err.Error())
	}
	return groups, nil
}

type flagCacheEntry struct {
	enabled            bool
	configuredGroupIds map[string]struct{}
	expiresAfter       time.Time
}

type featureFlagCache struct {
	mu        sync.Mutex
	flagCache interfaces.LRU[*flagCacheEntry]
}

func newCache() (*featureFlagCache, error) {
	flagConfig := &lru.Config[*flagCacheEntry]{
		MaxSize: flagCacheSize,
		SizeFn:  func(v *flagCacheEntry) int64 { return 1 },
	}
	flagCache, err := lru.NewLRU[*flagCacheEntry](flagConfig)
	if err != nil {
		return nil, err
	}
	return &featureFlagCache{
		flagCache: flagCache,
	}, nil
}

func (c *featureFlagCache) Get(flagName string) (e *flagCacheEntry, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.flagCache.Get(flagName)
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAfter) {
		c.flagCache.Remove(flagName)
		return nil, false
	}
	return entry, true
}

func (c *featureFlagCache) Add(flagName string, f *flagCacheEntry) {
	f.expiresAfter = time.Now().Add(*cacheTTL)
	c.mu.Lock()
	c.flagCache.Add(flagName, f)
	c.mu.Unlock()
}

func (c *featureFlagCache) UpdateExperimentAssignments(flagName string, experimentAssignments []string) (ok bool) {
	f, ok := c.Get(flagName)
	if !ok {
		return false
	}

	m := make(map[string]struct{})
	for _, e := range experimentAssignments {
		m[e] = struct{}{}
	}

	f.configuredGroupIds = m
	c.Add(flagName, f)

	return true
}

func (c *featureFlagCache) UpdateFlag(flagName string, enabled bool) (ok bool) {
	f, ok := c.Get(flagName)
	if !ok {
		return false
	}

	f.enabled = enabled
	c.Add(flagName, f)

	return true
}

func (ffs *FeatureFlagService) CreateGroups(ctx context.Context) error {
	for i := 0; i < 100; i++ {
		name, err := random.RandomString(8)
		if err != nil {
			return err
		}
		_, err = ffs.env.GetUserDB().CreateGroup(ctx, &tables.Group{
			Name: name,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
