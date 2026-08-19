package zu

// Build with -tags zu. libzu has to be built first, from a zu checkout
// next to this one:
//
//	cd ../zu && cargo build --release -p zu-capi
//	cd ../go-ycsb && go build -tags zu -o .bench/ycsb-zu ./cmd/go-ycsb
//
// The cgo lines in db.go point at ../../../zu, which is where the repos
// sit when they are checked out side by side. Anywhere else, hand the
// paths in:
//
//	CGO_CFLAGS="-I$ZU_INCLUDE" \
//	CGO_LDFLAGS="-L$ZU_LIB -lzu -Wl,-rpath,$ZU_LIB" \
//	  go build -tags zu ./cmd/go-ycsb
//
// The rpath matters. Without it the binary builds and then cannot find
// the library at run time, which looks like a missing engine rather than
// a missing rpath.
