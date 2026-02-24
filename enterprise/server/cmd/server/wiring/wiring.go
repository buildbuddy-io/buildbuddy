package wiring

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
)

type InternalEnvWiringFunc func(env *real_environment.RealEnv) error

var (
	mu                 sync.Mutex
	internalEnvWirings []InternalEnvWiringFunc
)

// RegisterInternalEnvWiring is called by buildbuddy-internal (typically from init())
// to register callbacks that wire internal services into env.
func RegisterInternalEnvWiring(fn InternalEnvWiringFunc) {
	if fn == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	internalEnvWirings = append(internalEnvWirings, fn)
}

// ApplyInternalEnvWiring is called by public server startup to run all
// registered internal wiring callbacks against the configured env.
func ApplyInternalEnvWiring(env *real_environment.RealEnv) error {
	mu.Lock()
	wirings := append([]InternalEnvWiringFunc(nil), internalEnvWirings...)
	mu.Unlock()
	for _, fn := range wirings {
		if err := fn(env); err != nil {
			return err
		}
	}
	return nil
}
