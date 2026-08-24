package pricing_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/pricing"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/stretchr/testify/require"
)

func TestPriceAt(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx := context.Background()
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(10 * 24 * time.Hour)
	p1 := pricing.Price{SKU: sku.RemoteCacheCASHits, EffectiveAt: t0, Quantity: 1000, PriceMicroDollars: 1}
	p2 := pricing.Price{SKU: sku.RemoteCacheCASHits, EffectiveAt: t1, Quantity: 1000, PriceMicroDollars: 2}
	for id, p := range map[string]pricing.Price{"BP1": p1, "BP2": p2} {
		require.NoError(t, te.GetDBHandle().NewQuery(ctx, "test_insert_price").Create(&tables.BillingPrice{
			BillingPriceID:    id,
			SKU:               p.SKU,
			EffectiveAtUsec:   p.EffectiveAt.UnixMicro(),
			Quantity:          p.Quantity,
			PriceMicroDollars: p.PriceMicroDollars,
		}))
	}

	for _, tc := range []struct {
		name string
		sku  sku.SKU
		at   time.Time
		want *pricing.Price
	}{
		{name: "before first price", sku: p1.SKU, at: t0.Add(-time.Hour), want: nil},
		{name: "first price", sku: p1.SKU, at: t0, want: &p1},
		{name: "between prices", sku: p1.SKU, at: t1.Add(-time.Hour), want: &p1},
		{name: "latest price", sku: p1.SKU, at: t1.Add(time.Hour), want: &p2},
		{name: "unpriced sku", sku: sku.BuildEventsBESCount, at: t1, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pricing.PriceAt(ctx, te.GetDBHandle(), tc.sku, tc.at)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	require.Error(t, te.GetDBHandle().NewQuery(ctx, "test_insert_duplicate_price").Create(&tables.BillingPrice{
		BillingPriceID: "BP3", SKU: p1.SKU, EffectiveAtUsec: t0.UnixMicro(), Quantity: 1, PriceMicroDollars: 1,
	}))
}
