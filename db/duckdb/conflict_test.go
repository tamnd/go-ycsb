//go:build duckdb

package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/magiconair/properties"
)

// The load path writes what it was given, which is not the tautology it
// sounds like. DuckDB 1.4.1 resolves the column list of an INSERT that
// carries a conflict clause case sensitively, so an adapter that names
// field0 against a table declared FIELD0 gets a row of NULLs and no
// error, and every read of that row afterwards is a fast read of nothing.
// This is the test that would have caught it. See tamnd/zu#551.
func TestALoadWritesTheFieldsItWasGiven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conflict.db")
	p := properties.NewProperties()
	p.Set("duckdb.dbpath", path)
	p.Set("threadcount", "1")
	p.Set("fieldcount", "10")
	d, err := duckdbCreator{}.Create(p)
	if err != nil {
		t.Fatal(err)
	}

	// Lower case, which is what the workload generates and is the half of
	// this that mattered.
	values := map[string][]byte{}
	for i := 0; i < 10; i++ {
		values[fmt.Sprintf("field%d", i)] = []byte("abcdefghij")
	}
	ctx := d.InitThread(context.Background(), 0, 1)
	for i := 0; i < 200; i++ {
		if err := d.Insert(ctx, "usertable", fmt.Sprintf("user%06d", i), values); err != nil {
			t.Fatal(err)
		}
	}
	d.CleanupThread(ctx)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.Remove(path + ".wal")
	}()
	var rows, empty int
	err = db.QueryRow(
		"select count(*), count(*) filter (where FIELD9 is null or FIELD0 is null) from usertable",
	).Scan(&rows, &empty)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 200 {
		t.Fatalf("the load wrote %d rows and it was given 200", rows)
	}
	if empty != 0 {
		t.Fatalf("%d of %d rows came back with no fields in them", empty, rows)
	}
}
