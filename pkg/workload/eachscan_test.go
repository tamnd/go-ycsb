package workload

import (
	"context"
	"testing"

	"github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// plainDB is a driver with no scan path of its own.
type plainDB struct{ ycsb.DB }

// eachDB is one that has it.
type eachDB struct{ ycsb.DB }

func (eachDB) ScanEach(ctx context.Context, table string, startKey string, count int, fields []string, fn func(values [][]byte) error) error {
	return nil
}

// The capability question has to reach the driver. The wrapper answers
// yes to it whatever is underneath, so asking the wrapper would put every
// driver on a path that only three of them implement, and the error would
// not show up until the scan ran.
func TestEachScannerAsksTheDriverAndNotTheWrapper(t *testing.T) {
	if _, ok := eachScanner(client.DbWrapper{DB: plainDB{}}); ok {
		t.Fatal("a driver with no ScanEach was offered the each path")
	}
	es, ok := eachScanner(client.DbWrapper{DB: eachDB{}})
	if !ok {
		t.Fatal("a driver with ScanEach was not offered the each path")
	}
	// And what comes back is the wrapper, because that is what times the
	// call. A bare driver here is a scan that never reaches the summary.
	if _, wrapped := es.(client.DbWrapper); !wrapped {
		t.Fatalf("the each path got %T, which does not measure anything", es)
	}
}

// An unwrapped driver is what the tests and any future caller hold, and
// it answers for itself.
func TestEachScannerTakesABareDriver(t *testing.T) {
	if _, ok := eachScanner(eachDB{}); !ok {
		t.Fatal("a bare driver with ScanEach was turned down")
	}
	if _, ok := eachScanner(plainDB{}); ok {
		t.Fatal("a bare driver with no ScanEach was accepted")
	}
}
