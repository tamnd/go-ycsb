package measurement

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type csventry struct {
	// start time of the operation in us from unix epoch
	startUs int64
	// latency of the operation in us
	latencyUs int64
}

type csvs struct {
	opCsv map[string][]csventry
}

func (c *csvs) GenerateExtendedOutputs() {
}

func InitCSV() *csvs {
	return &csvs{
		opCsv: make(map[string][]csventry),
	}
}

func (c *csvs) Measure(op string, start time.Time, lan time.Duration) {
	c.opCsv[op] = append(c.opCsv[op], csventry{
		startUs:   start.UnixMicro(),
		latencyUs: lan.Microseconds(),
	})
}

// merge folds another set of rows into this one.
//
// The rows are sorted by start time afterwards, so a merged file reads
// in the order the operations happened rather than in worker order.
// The single locked measurer produced arrival order, which for a
// multi threaded run was the same thing.
func (c *csvs) merge(other ycsb.Measurer) {
	o, ok := other.(*csvs)
	if !ok {
		return
	}
	for op, entries := range o.opCsv {
		c.opCsv[op] = append(c.opCsv[op], entries...)
	}
}

func (c *csvs) Output(w io.Writer) error {
	_, err := fmt.Fprintln(w, "operation,timestamp_us,latency_us")
	if err != nil {
		return err
	}
	for op, entries := range c.opCsv {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].startUs < entries[j].startUs
		})
		for _, entry := range entries {
			_, err := fmt.Fprintf(w, "%s,%d,%d\n", op, entry.startUs, entry.latencyUs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *csvs) Summary() {
	// do nothing as csvs don't keep a summary
}
