// +build !windows

package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"log/syslog"
	"sort"
	"time"
)

// Output each metric in the given registry to syslog periodically using
// the given syslogger.
func LoopLog(r Registry, d time.Duration, w *syslog.Writer) {
	for range time.Tick(d) {
		Log(r, w)
	}
}

func Log(r Registry, w *syslog.Writer) {
	r.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case Counter:
			w.Info(fmt.Sprintf("counter %s: count: %d", name, metric.Count()))
		case Gauge:
			w.Info(fmt.Sprintf("gauge %s: value: %d", name, metric.Value()))
		case GaugeFloat64:
			w.Info(fmt.Sprintf("gauge %s: value: %f", name, metric.Value()))
		case Healthcheck:
			metric.Check()
			w.Info(fmt.Sprintf("healthcheck %s: error: %v", name, metric.Error()))
		case Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			w.Info(fmt.Sprintf(
				"histogram %s: count: %d min: %d max: %d mean: %.2f stddev: %.2f "+
					"median: %.2f 75%%: %.2f 95%%: %.2f 99%%: %.2f 99.9%%: %.2f",
				name,
				h.Count(),
				h.Min(),
				h.Max(),
				h.Mean(),
				h.StdDev(),
				ps[0],
				ps[1],
				ps[2],
				ps[3],
				ps[4],
			))
		case Meter:
			m := metric.Snapshot()
			w.Info(fmt.Sprintf(
				"meter %s: count: %d 1-min: %.2f 5-min: %.2f 15-min: %.2f mean: %.2f",
				name,
				m.Count(),
				m.Rate1(),
				m.Rate5(),
				m.Rate15(),
				m.RateMean(),
			))
		case Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			w.Info(fmt.Sprintf(
				"timer %s: count: %d min: %d max: %d mean: %.2f stddev: %.2f median: %.2f 75%%: %.2f 95%%: %.2f 99%%: %.2f 99.9%%: %.2f 1-min: %.2f 5-min: %.2f 15-min: %.2f mean-rate: %.2f",
				name,
				t.Count(),
				t.Min(),
				t.Max(),
				t.Mean(),
				t.StdDev(),
				ps[0],
				ps[1],
				ps[2],
				ps[3],
				ps[4],
				t.Rate1(),
				t.Rate5(),
				t.Rate15(),
				t.RateMean(),
			))
		}
	})
}

func LoopWrite(r Registry, d time.Duration, w *syslog.Writer) {
	for range time.Tick(d) {
		WriteSorted(r, w)
	}
}

func WriteSorted(r Registry, w io.Writer) {
	var namedMetrics namedMetricSlice
	r.Each(func(name string, i interface{}) {
		namedMetrics = append(namedMetrics, namedMetric{name, i})
	})

	sort.Sort(namedMetrics)
	for _, namedMetric := range namedMetrics {
		switch metric := namedMetric.m.(type) {
		case Counter:
			fmt.Fprintf(w, "counter %s: count: %d", namedMetric.name, metric.Count())
		case Gauge:
			fmt.Fprintf(w, "gauge %s: value: %d", namedMetric.name, metric.Value())
		case GaugeFloat64:
			fmt.Fprintf(w, "gauge %s: value: %d", namedMetric.name, metric.Value())
		case Healthcheck:
			metric.Check()
			fmt.Fprintf(w, "healthcheck %s: error: %v", namedMetric.name, metric.Error())
		case Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			fmt.Fprintf(
				w, "histogram %s: count: %d min: %d max: %d mean: %.2f stddev: %.2f "+
					"median: %.2f 75%%: %.2f 95%%: %.2f 99%%: %.2f 99.9%%: %.2f",
				namedMetric.name,
				h.Count(),
				h.Min(),
				h.Max(),
				h.Mean(),
				h.StdDev(),
				ps[0],
				ps[1],
				ps[2],
				ps[3],
				ps[4],
			)
		case Meter:
			m := metric.Snapshot()
			fmt.Fprintf(w,
				"meter %s: count: %d 1-min: %.2f 5-min: %.2f 15-min: %.2f mean: %.2f",
				namedMetric.name,
				m.Count(),
				m.Rate1(),
				m.Rate5(),
				m.Rate15(),
				m.RateMean(),
			)
		case Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			fmt.Fprintf(w,
				"timer %s: count: %d min: %d max: %d mean: %.2f stddev: %.2f median: %.2f 75%%: %.2f 95%%: %.2f 99%%: %.2f 99.9%%: %.2f 1-min: %.2f 5-min: %.2f 15-min: %.2f mean-rate: %.2f",
				namedMetric.name,
				t.Count(),
				t.Min(),
				t.Max(),
				t.Mean(),
				t.StdDev(),
				ps[0],
				ps[1],
				ps[2],
				ps[3],
				ps[4],
				t.Rate1(),
				t.Rate5(),
				t.Rate15(),
				t.RateMean(),
			)
		}
	}
}

type namedMetric struct {
	name string
	m    interface{}
}

// namedMetricSlice is a slice of namedMetrics that implements sort.Interface.
type namedMetricSlice []namedMetric

func (nms namedMetricSlice) Len() int { return len(nms) }

func (nms namedMetricSlice) Swap(i, j int) { nms[i], nms[j] = nms[j], nms[i] }

func (nms namedMetricSlice) Less(i, j int) bool {
	return nms[i].name < nms[j].name
}

func MarshalJSON(r Registry) ([]byte, error) {
	data := make(map[string]map[string]interface{})
	r.Each(func(name string, i interface{}) {
		values := make(map[string]interface{})
		switch metric := i.(type) {
		case Counter:
			values["count"] = metric.Count()
		case Gauge:
			values["value"] = metric.Value()
		case GaugeFloat64:
			values["value"] = metric.Value()
		case Healthcheck:
			values["error"] = nil
			metric.Check()
			if err := metric.Error(); nil != err {
				values["error"] = metric.Error().Error()
			}
		case Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			values["count"] = h.Count()
			values["min"] = h.Min()
			values["max"] = h.Max()
			values["mean"] = h.Mean()
			values["stddev"] = h.StdDev()
			values["median"] = ps[0]
			values["75%"] = ps[1]
			values["95%"] = ps[2]
			values["99%"] = ps[3]
			values["99.9%"] = ps[4]
		case Meter:
			m := metric.Snapshot()
			values["count"] = m.Count()
			values["1m.rate"] = m.Rate1()
			values["5m.rate"] = m.Rate5()
			values["15m.rate"] = m.Rate15()
			values["mean.rate"] = m.RateMean()
		case Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			values["count"] = t.Count()
			values["min"] = t.Min()
			values["max"] = t.Max()
			values["mean"] = t.Mean()
			values["stddev"] = t.StdDev()
			values["median"] = ps[0]
			values["75%"] = ps[1]
			values["95%"] = ps[2]
			values["99%"] = ps[3]
			values["99.9%"] = ps[4]
			values["1m.rate"] = t.Rate1()
			values["5m.rate"] = t.Rate5()
			values["15m.rate"] = t.Rate15()
			values["mean.rate"] = t.RateMean()
		}
		data[name] = values
	})
	return json.Marshal(data)
}
