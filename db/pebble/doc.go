package pebble

// Pebble is behind a build tag like the other engines under measurement,
// so a binary built for one engine does not link another one's
// dependencies. It is pure Go and needs nothing installed on the host:
//
//	go build -tags pebble ./cmd/go-ycsb
