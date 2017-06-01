package metrics

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
)


var GlobRegistry Registry = DefaultRegistry
var registryMap map[string]Registry = make(map[string]Registry, 0)

func GetRegistry(key string) Registry {
	if m, ok := registryMap[key]; ok {
		return m
	}
	return NewRegistry()
}

func SetGlobRegistry(r Registry) {
	if GlobRegistry.Len() > 0 {
		panic("GlobRegistry metrics not empty")
	}
	GlobRegistry = r
}


const (
	GaugeOptionUpdate = iota
	GaugeOptionInc
	GaugeOptionDec
)

func NewBaseStat(prefix string, r Registry) *BaseStat {
	return &BaseStat{
		Prefix: prefix,
		Registry: r,
		Errs: make(map[string]*ErrorWithCount),
	}
}

type BaseStat struct {
	mu sync.RWMutex
	Prefix    string
	Registry  Registry
	Errs      map[string]*ErrorWithCount
}

type ErrorWithCount struct {
	Err   error
	Count int64
}

func (c *BaseStat) GetMemMetric(key string) string {
	return fmt.Sprintf("%s.%s", c.Prefix, key)
}

func (c *BaseStat) OnErr(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if e, ok := c.Errs[key]; ok {
		atomic.AddInt64(&e.Count, 1)
	}else {
		e = &ErrorWithCount{
			Err:   err,
		}
		c.Errs[key] = e
		atomic.StoreInt64(&c.Errs[key].Count, 1)
	}
	c.GaugeInc("error."+key, 1)
}

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
	m := c.Registry.GetOrRegister(c.GetMemMetric(key), NewGauge())
	// m := c.Registry.Get(c.GetMemMetric(key))
	if m != nil {
		switch op {
		case GaugeOptionInc:
			m.(Gauge).Inc(value)
		case GaugeOptionDec:
			m.(Gauge).Inc(value)
		case GaugeOptionUpdate:
			m.(Gauge).Update(value)
		default:
			m.(Gauge).Update(value)
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
	m := c.Registry.GetOrRegister(c.GetMemMetric(key), NewGaugeFloat64())
	// m := c.Registry.Get(c.GetMemMetric(key))
	if m != nil {
		m.(GaugeFloat64).Update(value)
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
	m := c.Registry.GetOrRegister(c.GetMemMetric(key), NewCounter())
	// m := c.Registry.Get(c.GetMemMetric(key))
	if m != nil {
		m.(Counter).Inc(value)
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
		m := c.Registry.GetOrRegister(c.GetMemMetric(key), NewCounter())
		// m := c.Registry.Get(c.GetMemMetric(key))
		if m != nil {
			m.(Counter).Inc(value - last)
		}
	}
	
	lastMap[k] = value
}