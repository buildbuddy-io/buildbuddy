package firecracker_test

// Tests for network mode derivation logic (CVE-candidate: network=off +
// init-dockerd bypass). No Firecracker binary or privileged resources required.

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNetworkMode_OffNoDockerD confirms the baseline: network=off without
// init-dockerd yields NETWORK_MODE_OFF (no network interface).
func TestNetworkMode_OffNoDockerD(t *testing.T) {
	mode, err := firecracker.NetworkMode("off", false)
	require.NoError(t, err)
	assert.Equal(t, fcpb.NetworkMode_NETWORK_MODE_OFF, mode,
		"network=off without init-dockerd must resolve to NETWORK_MODE_OFF")
}

// TestNetworkMode_OffWithInitDockerd is the core finding:
// network=off + init-dockerd=true currently resolves to NETWORK_MODE_LOCAL
// instead of NETWORK_MODE_OFF or an explicit error. This test DOCUMENTS
// (and thereby confirms) the security scanner's finding.
//
// If/when the vulnerability is remediated the expected value here should change
// to NETWORK_MODE_OFF (or the call should return an error).
func TestNetworkMode_OffWithInitDockerd(t *testing.T) {
	mode, err := firecracker.NetworkMode("off", true)
	require.NoError(t, err)

	// VULNERABILITY CONFIRMED: the combination silently escalates to LOCAL.
	// Change the assertion below after remediation.
	assert.Equal(t, fcpb.NetworkMode_NETWORK_MODE_LOCAL, mode,
		"CONFIRMED VULNERABILITY: network=off + init-dockerd=true resolves to "+
			"NETWORK_MODE_LOCAL (host-reachable) instead of NETWORK_MODE_OFF (no interface)")

	// Also assert the property the scanner cares about: networking is enabled.
	assert.True(t, firecracker.NetworkingEnabled(mode),
		"CONFIRMED VULNERABILITY: resulting mode has networking enabled, "+
			"bypassing the no-network isolation boundary")
}

// TestNetworkMode_OffWithInitDockerd_ShouldBeOff is the remediation target:
// network=off + init-dockerd=true should NOT produce a networking-enabled mode.
// This test will FAIL until the vulnerability is fixed, serving as a regression
// gate.
func TestNetworkMode_OffWithInitDockerd_ShouldBeOff(t *testing.T) {
	mode, err := firecracker.NetworkMode("off", true)

	// After remediation: either return an error, or return NETWORK_MODE_OFF.
	if err != nil {
		// Acceptable remediation: reject the combination with a clear error.
		return
	}

	// If no error, the mode must not have networking enabled.
	assert.False(t, firecracker.NetworkingEnabled(mode),
		"network=off must not produce a networking-enabled mode regardless of init-dockerd; "+
			"got mode=%v", mode)
}

// TestNetworkMode_ExternalVariants sanity-checks the non-off paths are unaffected.
func TestNetworkMode_ExternalVariants(t *testing.T) {
	cases := []struct {
		network     string
		initDockerd bool
		want        fcpb.NetworkMode
	}{
		{network: "external", initDockerd: false, want: fcpb.NetworkMode_NETWORK_MODE_EXTERNAL},
		{network: "external", initDockerd: true, want: fcpb.NetworkMode_NETWORK_MODE_EXTERNAL},
		{network: "", initDockerd: false, want: fcpb.NetworkMode_NETWORK_MODE_EXTERNAL},
		{network: "", initDockerd: true, want: fcpb.NetworkMode_NETWORK_MODE_EXTERNAL},
	}
	for _, tc := range cases {
		mode, err := firecracker.NetworkMode(tc.network, tc.initDockerd)
		require.NoError(t, err)
		assert.Equal(t, tc.want, mode, "network=%q initDockerd=%v", tc.network, tc.initDockerd)
	}
}

// TestNetworkMode_InvalidInput confirms unrecognised values are rejected.
func TestNetworkMode_InvalidInput(t *testing.T) {
	_, err := firecracker.NetworkMode("bogus", false)
	assert.Error(t, err)
}

// TestNetworkingEnabled_OffModeDisablesNetworking confirms NETWORK_MODE_OFF is
// the only mode where NetworkingEnabled returns false.
func TestNetworkingEnabled_OffModeDisablesNetworking(t *testing.T) {
	assert.False(t, firecracker.NetworkingEnabled(fcpb.NetworkMode_NETWORK_MODE_OFF))
	assert.True(t, firecracker.NetworkingEnabled(fcpb.NetworkMode_NETWORK_MODE_LOCAL))
	assert.True(t, firecracker.NetworkingEnabled(fcpb.NetworkMode_NETWORK_MODE_EXTERNAL))
	assert.True(t, firecracker.NetworkingEnabled(fcpb.NetworkMode_NETWORK_MODE_UNSPECIFIED))
}

// TestNetworkMode_SafetyCheckBypass confirms that the existing guard
// (InitDockerd requires NetworkingEnabled) is bypassed by the off+initDockerd
// combination. The guard at firecracker.go checks:
//
//	if opts.VMConfiguration.InitDockerd && !NetworkingEnabled(opts.VMConfiguration.NetworkMode)
//
// With network=off + init-dockerd=true the mode is LOCAL (NetworkingEnabled=true),
// so the guard does NOT fire even though the user requested no-network.
func TestNetworkMode_SafetyCheckBypass(t *testing.T) {
	mode, err := firecracker.NetworkMode("off", true)
	require.NoError(t, err)

	initDockerd := true
	// Replicate the guard condition from firecracker.go.
	guardWouldReject := initDockerd && !firecracker.NetworkingEnabled(mode)

	assert.False(t, guardWouldReject,
		"CONFIRMED: the existing InitDockerd safety guard does NOT fire for "+
			"network=off + init-dockerd=true because the resolved mode is LOCAL "+
			"(NetworkingEnabled=true). The guard is bypassed.")
}
