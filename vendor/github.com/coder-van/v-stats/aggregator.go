package statsd

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coder-van/v-stats/metrics"
	"github.com/coder-van/v-util/log"
)

/*
 数据以固定周期刷新，上次刷新据此次刷新为一个周期，在每次刷新时对不同类型数据做不同的聚合运算处理
*/
const Prefix = ""

var Percentiles = []float64{0.5, 0.75, 0.95, 0.99, 0.999}

func NewAggregator(flushSeconds int,
	r metrics.Registry,
	in chan []byte,
	out chan metrics.MetricDataPoint) *aggregator {

	return &aggregator{
		exit:            make(chan bool),
		FlushSeconds:    flushSeconds,
		metricsRegister: r,
		chanOut:         out,
		chanIn:          in,
		logger:          log.GetLogger("statsd", log.RotateModeMonth),
	}
}

type aggregator struct {
	exit            chan bool
	FlushSeconds    int
	metricsRegister metrics.Registry
	chanOut         chan metrics.MetricDataPoint
	chanIn          chan []byte
	logger          *log.Vlogger
}

func (agg *aggregator) HandlePackets(packet string) {
	/*
	 收到的 packets格式 由\n符分隔的多项数据
	 <metric>\n[metric...]
	 metric 内容如下
	 <key>:<section1>:[section2]
	 section:
	 <value>|<type>|@<rate>[|#<tag1_name=tag1_value>,[tag2_name=tag2_value]]
	*/
	_metricLines := strings.Split(packet, "\n")
	for _, metricLine := range _metricLines {
		metricLineTmp := strings.TrimSpace(metricLine)
		if metricLineTmp != "" {
			err := agg.parseMetricLine(metricLine)
			if err != nil {
				//log.Error("Error occurred when parsing packet:", err)
				continue
			}
		}
	}
}

func (agg *aggregator) parseMetricLine(packet string) error {
	/*
	 parse metric to
	*/
	bits := strings.SplitN(packet, ":", 2)
	if len(bits) != 2 {
		//log.Infof("Error: splitting ':', Unable to parse metric: %s", packet)
		return fmt.Errorf("Error Parsing statsd packet")
	}

	key := bits[0]
	metadata := bits[1]

	data := []string{}
	sections := strings.Split(metadata, ":")
	for _, token := range sections {
		data = append(data, token)
	}

	agg.parseSections(key, data)
	return nil
}

func (agg *aggregator) parseSections(key string, data []string) {
	// 将同一个key下的数据整理成数据数组
	for _, datum := range data {
		// Validate splitting the bit on "|"
		fields := strings.Split(datum, "|")
		if len(fields) < 2 {
			agg.logger.Printf("Error parsing packet Key: %s, data: %s", key, data)
		}

		// Set allows value of strings.
		metric_type := fields[1]
		value_str := fields[0]

		switch metric_type {
		case "c":
			c := metrics.NewCounter()
			c = agg.metricsRegister.GetOrRegister(key, c).(metrics.Counter)
			value, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil {
				agg.logger.Printf("parse int %s, %s", key, err)
			}
			c.Inc(value)
		case "g":
			if strings.Index(value_str, ".") > 0 {
				value, err := strconv.ParseFloat(fields[0], 64)
				if err != nil {
					agg.logger.Printf("parse int %s, %s", key, err)
				}
				g := metrics.NewGaugeFloat64()
				g = agg.metricsRegister.GetOrRegister(key, g).(metrics.GaugeFloat64)
				g.Update(value)
			} else {
				value, err := strconv.ParseInt(fields[0], 10, 64)
				if err != nil {
					agg.logger.Printf("parse int %s, %s", key, err)
				}
				g := metrics.NewGauge()
				g = agg.metricsRegister.GetOrRegister(key, g).(metrics.Gauge)
				g.Update(value)
			}
		case "ms", "h":
			value, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil {
				agg.logger.Printf("parse int %s, %s", key, err)
			}
			t := metrics.NewTimer()
			t = agg.metricsRegister.GetOrRegister(key, t).(metrics.Timer)
			t.Update(time.Duration(value * int64(time.Millisecond)))
		}
	}

}

func (agg *aggregator) Flush() {
	du := float64(time.Nanosecond)
	now := time.Now().Unix()
	seconds := agg.FlushSeconds
	agg.metricsRegister.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			agg.chanOut <- metrics.NewMetricDataPoint(name+".count", metric.Count(), now)
			// 计算每秒速度
			agg.chanOut <- metrics.NewMetricDataPoint(name+".rate", metric.Count()/int64(seconds), now)
			metric.Clear()
		case metrics.Gauge:
			agg.chanOut <- metrics.NewMetricDataPoint(name+".value", metric.Value(), now)
		case metrics.GaugeFloat64:
			agg.chanOut <- metrics.NewMetricDataPoint(name+".value", metric.Value(), now)
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles(Percentiles)
			agg.chanOut <- metrics.NewMetricDataPoint(name+".count", h.Count(), now)
			agg.chanOut <- metrics.NewMetricDataPoint(name+".min", h.Min(), now)
			agg.chanOut <- metrics.NewMetricDataPoint(name+".max", h.Max(), now)
			agg.chanOut <- metrics.NewMetricDataPoint(name+".mean", h.Mean(), now)
			agg.chanOut <- metrics.NewMetricDataPoint(name+".std-dev", h.StdDev(), now)

			for j, key := range Percentiles {
				key := strings.Replace(strconv.FormatFloat(key*100.0, 'f', -1, 64), ".", "", 1)
				agg.chanOut <- metrics.NewMetricDataPoint(name+"-percentile"+key, ps[j], now)
			}
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles(Percentiles)
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.count", Prefix, name), t.Count(), now)
			// rate
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.min", Prefix, name), t.Min()/int64(du), now)
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.max", Prefix, name), t.Max()/int64(du), now)
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.mean", Prefix, name), t.Mean()/du, now)
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.std-dev", Prefix, name), t.StdDev()/du, now)

			agg.chanOut <- metrics.NewMetricDataPoint(name+"count", t.Count(), now)

			for j, key := range Percentiles {
				key := strings.Replace(strconv.FormatFloat(key*100.0, 'f', -1, 64), ".", "", 1)
				k := fmt.Sprintf("%s.%s-percentile %.2f", Prefix, name, key)
				agg.chanOut <- metrics.NewMetricDataPoint(k, ps[j], now)
			}
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.1-minute", Prefix, name), t.Rate1(), now)
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.5-minute", Prefix, name), t.Rate5(), now)
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.15-minute", Prefix, name), t.Rate15(), now)
			agg.chanOut <- metrics.NewMetricDataPoint(fmt.Sprintf("%s.%s.mean-rate", Prefix, name), t.RateMean(), now)
		}

	})

}

func (agg *aggregator) run(shutdown chan bool, interval time.Duration) {
	defer close(agg.exit)

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	agg.logger.Println("Statsd aggregator started")
	var packet []byte
	for {
		select {
		case <-shutdown:
			agg.logger.Println("Statsd aggregator stoped")
			return
		case <-ticker.C:
			agg.Flush()
		case packet = <-agg.chanIn:
			agg.HandlePackets(string(packet))
		}
	}
}

func (agg *aggregator) Start() {
	agg.logger.Println("Statsd aggregator starting")
	go agg.run(agg.exit, time.Duration(1e9*agg.FlushSeconds))
}

func (agg *aggregator) Stop() {
	agg.logger.Println("Statsd aggregator stoping")
	agg.exit <- true
}
