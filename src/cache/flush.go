package cache

import (
	"sort"
	"time"

	"github.com/coder-van/v-carbon/src/common"
	"github.com/coder-van/v-util/log"
	"fmt"
)

type queueItem struct {
	pointBag *common.PointBag
	orderKey int64
}

type queue []queueItem

type byOrderKey queue

func (v byOrderKey) Len() int           { return len(v) }
func (v byOrderKey) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byOrderKey) Less(i, j int) bool { return v[i].orderKey < v[j].orderKey }

// SetWriteStrategy ...
func (c *Cache) SetWriteStrategy(s string) (err error) {
	
	switch s {
	case "max":
		c.writeStrategy = MaxStrategy
	case "sort":
		c.writeStrategy = TimeSortedStrategy
		//case "noop":
		//	c.writeStrategy = Noop
	default:
		return fmt.Errorf("Unknown write strategy '%s', should be one of: max, sort, noop", s)
	}
	return nil
}

// todo add timer stat
func (c *Cache) MakeChanForDB() {

	writeStrategy := c.writeStrategy
	c.stat.CounterInc("queue-build-times", 1)

	start := time.Now()

	orderKey := func(p *common.PointBag) int64 {
		return 0
	}

	switch writeStrategy {
	case MaxStrategy:
		orderKey = func(p *common.PointBag) int64 {
			return int64(len(p.Data))
		}
	case TimeSortedStrategy:
		orderKey = func(p *common.PointBag) int64 {
			return p.Data[0].Timestamp
		}
	}
	size := 0  // data point count
	l := c.Len() * 2  // point bag length
	queuePB := make(queue, l)
	index := int32(0)

	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.Lock()

		for _, cpb := range shard.items {
			p := cpb.GetPointBagForDb()
			c.logger.DebugFilter(fiterCpuTotal(cpb.Metric),
				"write data points ", p.Data)
			len_data := len(p.Data)
			size += len_data
			if len_data <= 0 {
				continue
			}
			if index < l {
				queuePB[index].pointBag = p
				queuePB[index].orderKey = orderKey(p)
			} else {
				// it maybe impossible
				queuePB = append(queuePB, queueItem{p, orderKey(p)})
			}
			index++
		}

		shard.Unlock()
	}

	queuePB = queuePB[:index]

	switch writeStrategy {
	case MaxStrategy:
		sort.Sort(sort.Reverse(byOrderKey(queuePB)))
	case TimeSortedStrategy:
		sort.Sort(byOrderKey(queuePB))
	}

	
	if l == 0 || size == 0{
		return
	}

	for _, queueItem := range queuePB {
		// todo channel size fixed and
		c.ChanForDB <- queueItem.pointBag
	}
	
	logger := log.GetLogger("cache", log.RotateModeMonth)
	logger.Printf("make %d to queue, use: %s \n", size, time.Since(start))

}
