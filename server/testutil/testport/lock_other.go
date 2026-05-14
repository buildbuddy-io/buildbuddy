//go:build !unix

package testport

// lockPort is a no-op on non-Unix systems. Port collisions from parallel test
// processes are possible but unlikely without flock support.
func lockPort(port int) bool {
	return true
}
