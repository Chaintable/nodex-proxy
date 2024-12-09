package utils

import "math/rand"

// RangeRandom return a random int64 between [min,max)
func RangeRandom(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}
