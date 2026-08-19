package ladybug

// Build with -tags ladybug. LadybugDB has to be installed first, because
// unlike DuckDB the Go side does not bundle the library.
//
//	brew install ladybug
//
// The cgo directives in db.go point at the Homebrew keg on Apple silicon.
// On Linux, or anywhere else the library is not in that prefix, pass the
// paths at build time instead:
//
//	CGO_CFLAGS=-I/usr/local/include \
//	CGO_LDFLAGS="-L/usr/local/lib -llbug" \
//	  go build -tags ladybug ./cmd/go-ycsb
//
// Homebrew keeps ladybug as a keg that is not linked into
// /opt/homebrew/lib, so looking there and finding nothing does not mean
// it is missing. Check with brew --prefix ladybug.
