package register_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/cli_command/register"
	"github.com/stretchr/testify/require"
)

// Register panics if the declared commands and the registered handlers get out
// of sync, so this also guards against that.
func TestRegisterAttachesHandlers(t *testing.T) {
	register.Register()

	require.NotEmpty(t, cli_command.Commands)
	for _, command := range cli_command.Commands {
		require.NotNil(t, command.Handler, "command %q has no handler", command.Name)
	}
}
