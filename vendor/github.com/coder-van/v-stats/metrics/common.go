package metrics

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

func MakeMetric(name string, tags map[string]string) string {
	return fmt.Sprintf("%s.%s", name, StringifyTags(tags))
}

func StringifyTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}

	str := ""
	params := []string{}
	for key, value := range tags {
		str = key + "=" + value
		params = append(params, str)
	}
	sort.Strings(params)

	return strings.Join(params, ",")
}

type Metric interface {
	Name() string
	Metric() string
	GetTagsMap() map[string]string
	StringifyTags() string
	Snapshot() Metric
}

func NewMetricDataPoint(key string, value interface{}, t int64) MetricDataPoint {
	return MetricDataPoint{
		Key:       key,
		Value:     value,
		Timestamp: t,
	}
}

type MetricDataPoint struct {
	Key       string
	Value     interface{}
	Timestamp int64
}

func (dp MetricDataPoint) String() string {
	switch value := dp.Value.(type) {
	case int64:
		return fmt.Sprintf("%s %d %d\n", dp.Key, int64(value), dp.Timestamp)
	case float64:
		return fmt.Sprintf("%s %f %d\n", dp.Key, float64(value), dp.Timestamp)
	}

	panic("MetricDataPoint.Value type error")
	//return ""
}

func calculateDelta(oldValue, newValue int64) int64 {
	if oldValue < newValue {
		return newValue - oldValue
	} else {
		return (math.MaxInt64 - oldValue) + (newValue - math.MinInt64) + 1
	}
}
