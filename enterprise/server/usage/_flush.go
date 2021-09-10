func (ut *tracker) Flush(ctx context.Context) error {
	db := ut.env.GetDBHandle()

	batch := map[string]*tables.Usage{}

	flushBatch := func() error {
		usages := make([]*tables.Usage, 0, len(batch))
		for _, usage := range batch {
			usages = append(usages, usage)
		}
		c := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "period_start_usec"}},
			DoNothing: true,
		})
		if err := c.Create(usages).Error; err != nil {
			return err
		}
		for k, _ := range batch {
			// Once we've written to the DB, delete the usage values from Redis.
			// They won't be written again, since we only flush values from past
			// periods.
			if _, err := ut.rdb.Del(ctx, k).Result(); err != nil {
				return err
			}
		}
		batch = map[string]*tables.Usage{}
		return nil
	}

	iter := ut.rdb.Scan(ctx, 0, redisUsageKeyPrefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		tu := &tables.Usage{}
		if err := parseRedisKey(key, tu); err != nil {
			log.Error(err.Error())
			continue
		}
		counts, err := ut.readCounts(ctx, key)
		if err != nil {
			return err
		}
		tu.UsageCounts = *counts
		batch[key] = tu
		if len(batch) > flushBatchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		if err := flushBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (ut *tracker) readCounts(ctx context.Context, key string) (*tables.UsageCounts, error) {
	h, err := ut.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	counts := &tables.UsageCounts{}
	for hashKey, countRef := range redisHashFieldMap {
		str := h[hashKey]
		if str == "" {
			continue
		}
		c, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("Invalid usage count value stored in Redis: %q", str)
		}
		countPtr := countRef(counts)
		*countPtr = c
	}
	return counts, nil
}

func startOfUsagePeriodUTC(t time.Time) time.Time {
	utc := t.UTC()
	// Start of hour
	return time.Date(
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), 0, 0, 0,
		utc.Location())
}

// Given a key like "usage/values/GR123/2020-01-01T00:00:00Z", copies the group
// ID and usage period from the key into the given usage row.
func parseRedisKey(key string, u *tables.Usage) error {
	parts := strings.Split(key, "/")
	if len(parts) != 4 {
		return status.InvalidArgumentErrorf("Invalid key %q", key)
	}
	u.GroupID = parts[2]
	t, err := time.Parse(time.RFC3339, parts[3])
	if err != nil {
		return err
	}
	u.PeriodStartUsec = timeutil.ToUsec(t)
	return nil
}