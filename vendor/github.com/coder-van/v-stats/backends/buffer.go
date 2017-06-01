package backends

import (
	"github.com/coder-van/v-stats/metrics"
)

// Buffer is an object for storing metrics in a circular buffer.
type Buffer struct {
	buf chan metrics.MetricDataPoint
	// total dropped metrics
	drops int
	// total metrics added
	total int
}

// NewBuffer returns a Buffer
//   size is the maximum number of metrics that Buffer will cache. If Add is
//   called when the buffer is full, then the oldest metric(s) will be dropped.
func NewBuffer(size int) *Buffer {
	return &Buffer{
		buf: make(chan metrics.MetricDataPoint, size),
	}
}

// IsEmpty returns true if Buffer is empty.
func (b *Buffer) IsEmpty() bool {
	return len(b.buf) == 0
}

// Len returns the current length of the buffer.
func (b *Buffer) Len() int {
	return len(b.buf)
}

// Drops returns the total number of dropped metrics that have occurred in this
// buffer since instantiation.
func (b *Buffer) Drops() int {
	return b.drops
}

// Total returns the total number of metrics that have been added to this buffer.
func (b *Buffer) Total() int {
	return b.total
}

// Add adds metrics to the buffer.
func (b *Buffer) Add(metrics ...metrics.MetricDataPoint) {
	for i := range metrics {
		b.total++
		select {
		case b.buf <- metrics[i]:
		default:
			b.drops++
			<-b.buf
			b.buf <- metrics[i]
		}
	}
}

// Batch returns a batch of metrics of size batchSize.
// the batch will be of maximum length batchSize. It can be less than batchSize,
// if the length of Buffer is less than batchSize.
func (b *Buffer) Batch(batchSize int) []byte {
	n := min(len(b.buf), batchSize)
	// out := make([]metrics.MetricDataPoint, n)

	bs := make([]byte, 0, 128*n)
	for i := 0; i < n; i++ {
		dp := <-b.buf
		bs = append(bs, []byte(dp.String())...)
	}
	return bs
}

func min(a, b int) int {
	if b < a {
		return b
	}
	return a
}
