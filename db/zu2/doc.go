package zu2

// Build with -tags zu2. libzu2 has to be built first, from a zu
// checkout next to this one:
//
//	cd ../zu && cargo build --release -p zu2-capi
//	cd ../go-ycsb && go build -tags zu2 -o .bench/ycsb-zu2 ./cmd/go-ycsb
//
// The cgo lines in db.go point at ../../../zu, which is where the repos
// sit when they are checked out side by side. Anywhere else, hand the
// paths in:
//
//	CGO_CFLAGS="-I$ZU2_INCLUDE" \
//	CGO_LDFLAGS="-L$ZU2_LIB -lzu2 -Wl,-rpath,$ZU2_LIB" \
//	  go build -tags zu2 ./cmd/go-ycsb
//
// The rpath matters. Without it the binary builds and then cannot find
// the library at run time, which looks like a missing engine rather
// than a missing rpath.
//
// zu2 is the other engine in the same repo and not a newer libzu, so
// this adapter and db/zu can be built into one binary and both tags
// given at once. They register under different names and open different
// files.
