package sqlite

// Build with -tags sqlite. The driver bundles its own SQLite amalgamation,
// so the version is whatever github.com/mattn/go-sqlite3 pins in go.mod
// rather than whatever the host happens to ship. That is deliberate: this
// fork compares engines at pinned versions across macOS, Linux and Windows,
// and a system library would float per host.
//
// The upstream tag was libsqlite3, which is also the tag go-sqlite3 reads to
// link the system library instead of the bundled one. Keeping that name
// would have made it impossible to select the adapter without also giving up
// the pin, so the adapter gets its own tag.
