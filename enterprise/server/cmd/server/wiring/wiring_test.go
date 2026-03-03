package wiring_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/server/wiring"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
)

func TestApplyInternalEnvWiring_FromInitRegistration(t *testing.T) {
	env := real_environment.NewBatchEnv()
	var called bool
	wiring.RegisterInternalEnvWiring(func(got *real_environment.RealEnv) error {
		if got != env {
			t.Fatal("expected callback to receive target env")
		}
		called = true
		return nil
	})
	if err := wiring.ApplyInternalEnvWiring(env); err != nil {
		t.Fatalf("apply internal wiring: %s", err)
	}
	if !called {
		t.Fatal("expected registered callback to be invoked")
	}
}
