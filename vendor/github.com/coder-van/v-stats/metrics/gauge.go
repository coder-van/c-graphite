/*
 * Includes code from
 * https://raw.githubusercontent.com/rcrowley/go-metrics/master/sample.go
 * Copyright 2012 Richard Crowley. All rights reserved.
 */

package metrics

import "sync/atomic"

// Gauges hold an int64 value that can be set arbitrarily.
type Gauge interface {
	Snapshot() Gauge
	Update(int64)
	Dec(int64)
	Inc(int64)
	Value() int64
}

// GetOrRegisterGauge returns an existing Gauge or constructs and registers a
// new StandardGauge.
func GetOrRegisterGauge(name string, r Registry) Gauge {
	if nil == r {
		r = GlobRegistry
	}
	return r.GetOrRegister(name, NewGauge).(Gauge)
}

// NewGauge constructs a new StandardGauge.
func NewGauge() Gauge {
	//if UseNilMetrics {
	//	return NilGauge{}
	//}
	return &StandardGauge{0}
}

// NewRegisteredGauge constructs and registers a new StandardGauge.
func NewRegisteredGauge(name string, r Registry) Gauge {
	c := NewGauge()
	if nil == r {
		r = GlobRegistry
	}
	r.Register(name, c)
	return c
}

// GaugeSnapshot is a read-only copy of another Gauge.
type GaugeSnapshot int64

// Snapshot returns the snapshot.
func (g GaugeSnapshot) Snapshot() Gauge { return g }

// Update panics.
func (GaugeSnapshot) Update(int64) {
	panic("Update called on a GaugeSnapshot")
}

// Value returns the value at the time the snapshot was taken.
func (g GaugeSnapshot) Value() int64 { return int64(g) }

// Dec decrements the counter by the given amount.
func (g GaugeSnapshot) Dec(i int64) {
	panic("Dec called on a GaugeSnapshot")
}

// Inc increments the counter by the given amount.
func (g GaugeSnapshot) Inc(i int64) {
	panic("Inc called on a GaugeSnapshot")
}


type StandardGauge struct {
	value int64
}

// Snapshot returns a read-only copy of the gauge.
func (g *StandardGauge) Snapshot() Gauge {
	return GaugeSnapshot(g.Value())
}

// Update updates the gauge's value.
func (g *StandardGauge) Update(v int64) {
	atomic.StoreInt64(&g.value, v)
}

// Value returns the gauge's current value.
func (g *StandardGauge) Value() int64 {
	return atomic.LoadInt64(&g.value)
}

// Dec decrements the value by the given amount.
func (g *StandardGauge) Dec(i int64) {
	atomic.AddInt64(&g.value, -i)
}

// Inc increments the value by the given amount.
func (g *StandardGauge) Inc(i int64) {
	atomic.AddInt64(&g.value, i)
}
