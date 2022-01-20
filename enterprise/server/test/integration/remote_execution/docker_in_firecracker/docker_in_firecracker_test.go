package docker_in_firecracker_test

import (
	"context"
	"fmt"
	"os/exec"
)

func TestDockerInFirecracker_Busybox(t *testing.T) {
	fmt.Println("TestDockerInFirecracker")
	_ = exec.Command("docker", "ps")
}
