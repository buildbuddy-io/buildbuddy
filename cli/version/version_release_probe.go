package main

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

func main() {
	fmt.Print(version.String())
}
