package config

import "github.com/buildbuddy-io/buildbuddy/server/util/flag"

var billingEnabled = flag.Bool("app.enable_billing", false,
	"If set, enables self-serve billing via Stripe.")

func BillingEnabled() bool { return *billingEnabled }
