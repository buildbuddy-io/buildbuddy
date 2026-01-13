package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("HELLO WORLD")
	fmt.Println("Starting countdown...")

	for i := 5; i > 0; i-- {
		fmt.Printf("  %d...\n", i)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("Done!")
}
