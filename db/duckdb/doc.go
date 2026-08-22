package duckdb

// Build with -tags duckdb. The driver bundles its own DuckDB library, so
// the engine version is whatever github.com/duckdb/duckdb-go pins in
// go.mod and is the same on every host. Nothing needs to be installed.
//
// That is DuckDB's own driver. It used to live at marcboeker/go-duckdb,
// which is where this adapter first pointed, and that repository is now
// archived: the project moved into the duckdb organisation and the
// releases moved with it. The version numbering says which DuckDB is
// inside, so v2.10505.0 is DuckDB 1.5.5, and the archived path stopped
// at a DuckDB two minor versions older. Benchmarking the old one and
// calling it DuckDB would have been benchmarking an abandoned fork.
//
// Building takes a while the first time because the bundled library is a
// large static archive and cgo has to link it.
