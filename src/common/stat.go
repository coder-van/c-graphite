package common

import (
	statsd "github.com/coder-van/v-stats"
	"github.com/coder-van/v-stats/metrics"
	"fmt"
	"strings"
	"strconv"
	"time"
)

var Percentiles  = []float64{0.5, 0.75, 0.95, 0.99, 0.999}

var collectorMap map[string]*statsd.BaseStat = make(map[string]*statsd.BaseStat)
var Registry = metrics.NewRegistry()

func GetStat(prefix string) ( *statsd.BaseStat) {
	key := fmt.Sprintf("carbon.%s", prefix)
	if collector, ok := collectorMap[key]  ; ok {
		return collector
	}
	
	collector := statsd.NewBaseStat(key, Registry)
	collectorMap[key] = collector
	return collector
}


func Flush(cacheAdd func(MetricPoint), seconds time.Duration)  {
	du := float64(time.Nanosecond)
	now := time.Now().Unix()
	Registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			cacheAdd(MetricPoint{name+".count",  float64(metric.Count()), now})
			// rate per second
			cacheAdd(MetricPoint{name+".rate",  float64(metric.Count()/int64(seconds)), now})
			metric.Clear()
		case metrics.Gauge:
			cacheAdd(MetricPoint{name+".value",  float64(metric.Value()), now})
		case metrics.GaugeFloat64:
			cacheAdd(MetricPoint{name+".value",  metric.Value(), now})
		case metrics.Histogram:
			h := metric.Snapshot()
			
			cacheAdd(MetricPoint{name+".count",  float64(h.Count()), now})
			cacheAdd(MetricPoint{name+".min", float64(h.Min()), now})
			cacheAdd(MetricPoint{name+".max", float64(h.Max()), now})
			cacheAdd(MetricPoint{name+".mean", h.Mean(), now})
			cacheAdd(MetricPoint{name+".std-dev", h.StdDev(), now})
			
			ps := h.Percentiles(Percentiles)
			for j, key := range Percentiles {
				key := strings.Replace(strconv.FormatFloat(key*100.0, 'f', -1, 64), ".", "", 1)
				cacheAdd(MetricPoint{name+"-percentile_"+key, ps[j], now})
			}
		case metrics.Timer:
			t := metric.Snapshot()
			cacheAdd(MetricPoint{fmt.Sprintf("%s.count", name),  float64(t.Count()), now})
			cacheAdd(MetricPoint{fmt.Sprintf("%s.min", name), float64(t.Min())/du, now})
			cacheAdd(MetricPoint{fmt.Sprintf("%s.max", name), float64(t.Max())/du, now})
			cacheAdd(MetricPoint{fmt.Sprintf("%s.mean", name), t.Mean()/(du), now})
			cacheAdd(MetricPoint{fmt.Sprintf("%s.std-dev",name), t.StdDev()/(du), now})
			
			ps := t.Percentiles(Percentiles)
			for j, key := range Percentiles {
				key := strings.Replace(strconv.FormatFloat(key*100.0, 'f', -1, 64), ".", "", 1)
				k := fmt.Sprintf("%s.%s-percentile %.2f", name, key, key)
				cacheAdd(MetricPoint{k, ps[j], now})
			}
			cacheAdd(MetricPoint{fmt.Sprintf("%s.1-minute", name), t.Rate1(), now})
			cacheAdd(MetricPoint{fmt.Sprintf("%s.5-minute", name), t.Rate5(), now})
			cacheAdd(MetricPoint{fmt.Sprintf("%s.15-minute", name), t.Rate15(), now})
			cacheAdd(MetricPoint{fmt.Sprintf("%s.mean-rate", name), t.RateMean(), now})
		}
		
	})
}


