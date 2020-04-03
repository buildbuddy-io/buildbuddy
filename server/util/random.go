package random

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

var once sync.Once

func RandUint64() uint64 {
	once.Do(func() {
		rand.Seed(time.Now().UnixNano())
		log.Printf("Seeded random with current time!")
	})
	return rand.Uint64()
}
