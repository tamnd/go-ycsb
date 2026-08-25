package measurement

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type histograms struct {
	p *properties.Properties

	histograms map[string]*histogram
}

func (h *histograms) GenerateExtendedOutputs() {
	exportHistograms := h.p.GetBool(prop.MeasurementHistogramPercentileExport, prop.MeasurementHistogramPercentileExportDefault)
	if exportHistograms {
		exportHistogramsFilepath := h.p.GetString(prop.MeasurementHistogramPercentileExportFilepath, prop.MeasurementHistogramPercentileExportFilepathDefault)
		for op, opM := range h.histograms {
			outFile := fmt.Sprintf("%s%s-percentiles.txt", exportHistogramsFilepath, op)
			fmt.Printf("Exporting the full latency spectrum for operation '%s' in percentile output format into file: %s.\n", op, outFile)
			f, err := os.Create(outFile)
			if err != nil {
				panic("failed to create percentile output file: " + err.Error())
			}
			defer f.Close()
			w := bufio.NewWriter(f)
			_, err = opM.hist.PercentilesPrint(w, 1, 1.0)
			w.Flush()
			if err != nil {
				panic("failed to print percentiles: " + err.Error())
			}
		}
	}
}

func (h *histograms) Measure(op string, start time.Time, lan time.Duration) {
	opM, ok := h.histograms[op]
	if !ok {
		opM = newHistogram()
		h.histograms[op] = opM
	}

	opM.Measure(lan)
}

// merge folds another set of per operation histograms into this one.
//
// An hdr histogram merges exactly, so a percentile off the merged set
// is the percentile of every sample the run took, the same number the
// single locked histogram used to hold. The start time has to go back
// to the earlier of the two, because the elapsed seconds and therefore
// the OPS column are measured from it.
func (h *histograms) merge(other ycsb.Measurer) {
	o, ok := other.(*histograms)
	if !ok {
		return
	}
	for op, oM := range o.histograms {
		opM, ok := h.histograms[op]
		if !ok {
			opM = newHistogram()
			opM.startTime = oM.startTime
			h.histograms[op] = opM
		} else if oM.startTime.Before(opM.startTime) {
			opM.startTime = oM.startTime
		}
		opM.hist.Merge(oM.hist)
	}
}

func (h *histograms) summary() map[string][]string {
	summaries := make(map[string][]string, len(h.histograms))
	for op, opM := range h.histograms {
		summaries[op] = opM.Summary()
	}
	return summaries
}

func (h *histograms) Summary() {
	h.Output(os.Stdout)
}

// errorNotes is one line per operation type that had a failure in it,
// saying how many of the attempts failed.
//
// A failed operation is measured as OP_ERROR, so the summary of a run
// where every scan failed has a SCAN_ERROR line carrying a latency
// distribution and no SCAN line at all. Read quickly that is an engine
// that does not scan. Read by a script that greps for ^SCAN it is a
// missing cell. It is neither: it is a run that produced no result for
// that operation, and the latencies on the ERROR line are the cost of
// failing rather than the cost of the work. 4cc4b14 made the first error
// of each type reach stderr, which says why; this says how much.
//
// The distinction that matters is total against partial. A run with a
// handful of failures is still a result and the line is a footnote. A
// run where every attempt failed is not a result and presenting it as
// one is the hole this closes, which is the same hole as tamnd/zu#551
// from the other side: that one caught an engine succeeding and
// returning nothing, and nothing was catching an engine failing.
func (h *histograms) errorNotes() []string {
	const suffix = "_ERROR"
	ops := make([]string, 0, len(h.histograms))
	for op := range h.histograms {
		if strings.HasSuffix(op, suffix) {
			ops = append(ops, op)
		}
	}
	sort.Strings(ops)

	notes := make([]string, 0, len(ops))
	for _, op := range ops {
		base := strings.TrimSuffix(op, suffix)
		failed := h.histograms[op].hist.TotalCount()
		if failed == 0 {
			continue
		}
		var ok int64
		if m, has := h.histograms[base]; has {
			ok = m.hist.TotalCount()
		}
		total := ok + failed
		if ok == 0 {
			notes = append(notes, fmt.Sprintf(
				"# %s: every one of the %d attempts failed, so this run has no %s result. The %s latencies are the cost of failing.",
				base, failed, base, op))
			continue
		}
		notes = append(notes, fmt.Sprintf(
			"# %s: %d of %d attempts failed (%.2f%%), and the %s line is over the %d that did not.",
			base, failed, total, float64(failed)*100/float64(total), base, ok))
	}
	return notes
}

func (h *histograms) Output(w io.Writer) error {
	summaries := h.summary()
	keys := make([]string, 0, len(summaries))
	for k := range summaries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lines := [][]string{}
	for _, op := range keys {
		line := []string{op}
		line = append(line, summaries[op]...)
		lines = append(lines, line)
	}

	outputStyle := h.p.GetString(prop.OutputStyle, util.OutputStylePlain)
	switch outputStyle {
	case util.OutputStylePlain:
		util.RenderString(w, "%-6s - %s\n", header, lines)
	case util.OutputStyleJson:
		util.RenderJson(w, header, lines)
	case util.OutputStyleTable:
		util.RenderTable(w, header, lines)
	default:
		panic("unsupported outputstyle: " + outputStyle)
	}
	return nil
}

func InitHistograms(p *properties.Properties) *histograms {
	return &histograms{
		p:          p,
		histograms: make(map[string]*histogram, 16),
	}
}
