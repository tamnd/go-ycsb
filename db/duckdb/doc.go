package duckdb

// Build with -tags duckdb. The driver bundles its own DuckDB library, so
// the engine version is whatever github.com/marcboeker/go-duckdb pins in
// go.mod and is the same on every host. Nothing needs to be installed.
//
// Building takes a while the first time because the bundled library is a
// large static archive and cgo has to link it.
