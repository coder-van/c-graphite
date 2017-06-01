package statsd

import (
	"fmt"
	"sync/atomic"
	"github.com/coder-van/v-stats/metrics"
	"strconv"
	"sync"
)


func NewBaseStat(prefix string, r metrics.Registry) *BaseStat {
	return &BaseStat{
		Prefix: prefix,
		Registry: r,
		Stat: make(map[string]*ErrorWithCount),
	}
}

type BaseStat struct {
	mu sync.RWMutex
	Prefix    string
	Registry  metrics.Registry
	Stat      map[string]*ErrorWithCount
}

type ErrorWithCount struct {
	err   error
	count int64
}

func (c *BaseStat) GetMemMetric(key string) string {
	return fmt.Sprintf("%s.%s", c.Prefix, key)
}

func (c *BaseStat) OnErr(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.Stat[key]; ok {
		atomic.AddInt64(&e.count, 1)
	}else {
		e = &ErrorWithCount{
			err:   err,
		}
		c.Stat[key] = e
		atomic.StoreInt64(&c.Stat[key].count, 1)
	}
}

const (
	GaugeOptionUpdate = iota
	GaugeOptionInc
	GaugeOptionDec
)

func (c *BaseStat) gaugeOption(key string, i interface{}, op int) {
	var value int64
	var err error
	switch v := i.(type) {
	case int64:
		value = v
	case int:
		value = int64(v)
	case float64:
		value = int64(v)
	case string:
		value, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return
		}
	default:
		return
	}
	m := c.Registry.GetOrRegister(c.GetMemMetric(key), metrics.NewGauge())
	// m := c.Registry.Get(c.GetMemMetric(key))
	if m != nil {
		switch op {
		case GaugeOptionInc:
			m.(metrics.Gauge).Inc(value)
		case GaugeOptionDec:
			m.(metrics.Gauge).Inc(value)
		case GaugeOptionUpdate:
			m.(metrics.Gauge).Update(value)
		default:
			m.(metrics.Gauge).Update(value)
		}
	}
	
}

func (c *BaseStat) GaugeUpdate(key string, i interface{}){
	c.gaugeOption(key, i, GaugeOptionUpdate)
}

func (c *BaseStat) GaugeInc(key string, i interface{}){
	c.gaugeOption(key, i, GaugeOptionInc)
}

func (c *BaseStat) GaugeDec(key string, i interface{}){
	c.gaugeOption(key, i, GaugeOptionDec)
}

func (c *BaseStat) GaugeFloat64Update(key string, i interface{}) {
	var value float64
	var err error
	switch v := i.(type) {
	case float64:
		value = v
	case string:
		value, err = strconv.ParseFloat(v, 10)
		if err != nil {
			return
		}
	default:
		return
	}
	m := c.Registry.GetOrRegister(c.GetMemMetric(key), metrics.NewGaugeFloat64())
	// m := c.Registry.Get(c.GetMemMetric(key))
	if m != nil {
		m.(metrics.GaugeFloat64).Update(value)
	}
	
}

func (c *BaseStat) CounterInc(key string, i interface{}) {
	var value int64
	var err error
	switch v := i.(type) {
	case int64:
		value = v
	case int:
		value = int64(v)
	case float64:
		value = int64(v)
	case string:
		value, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return
		}
	default:
		return
	}
	m := c.Registry.GetOrRegister(c.GetMemMetric(key), metrics.NewCounter())
	// m := c.Registry.Get(c.GetMemMetric(key))
	if m != nil {
		m.(metrics.Counter).Inc(value)
	}
}

var lastMap map[string]int64= make(map[string]int64)

func (c *BaseStat) CounterIncTotal(key string, i interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	var value int64
	var err error
	switch v := i.(type) {
	case int64:
		value = v
	case float64:
		value = int64(v)
	case string:
		value, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return
		}
	default:
		return
		
	}
	k := c.GetMemMetric(key)
	if last, ok := lastMap[k]; ok {
		m := c.Registry.GetOrRegister(c.GetMemMetric(key), metrics.NewCounter())
		// m := c.Registry.Get(c.GetMemMetric(key))
		if m != nil {
			m.(metrics.Counter).Inc(value - last)
		}
	}
	
	lastMap[k] = value
}