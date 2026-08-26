// Package pricing reads usage SKU prices from the BillingPrices table.
package pricing

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
)

// Price is the price of a SKU from EffectiveAt onward: PriceMicroDollars per
// Quantity units of usage.
type Price struct {
	SKU               sku.SKU
	EffectiveAt       time.Time
	Quantity          int64
	PriceMicroDollars int64
}

// PriceAt returns the price of a SKU in effect at the given time, or nil if
// the SKU has no price.
func PriceAt(ctx context.Context, dbh interfaces.DBHandle, s sku.SKU, at time.Time) (*Price, error) {
	row := &tables.BillingPrice{}
	err := dbh.NewQuery(ctx, "pricing_price_at").Raw(`
		SELECT * FROM "BillingPrices"
		WHERE sku = ? AND effective_at_usec <= ?
		ORDER BY effective_at_usec DESC LIMIT 1`, s, at.UnixMicro()).Take(row)
	if db.IsRecordNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &Price{
		SKU:               row.SKU,
		EffectiveAt:       time.UnixMicro(row.EffectiveAtUsec).UTC(),
		Quantity:          row.Quantity,
		PriceMicroDollars: row.PriceMicroDollars,
	}, nil
}
