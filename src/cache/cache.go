package cache

/*
Based on https://github.com/orcaman/concurrent-map
*/

import (
	"sync"
	"sync/atomic"

	"github.com/coder-van/v-carbon/src/common"
	statsd "github.com/coder-van/v-stats"
	"time"
	"github.com/coder-van/v-util/log"
	"strings"
)

type WriteStrategy int

const (
	MaxStrategy WriteStrategy = iota
	TimeSortedStrategy
)

const shardCount = 1024

type CachePointBag struct {
	sync.RWMutex
	common.PointBag
	// persistTime  int64  // time record persist
	PointsToDb  []common.Point
	duration    int64  // seconds from now point cache, for cache hit
}

// Add point to list with sort by timestamp
func (cpb *CachePointBag) Add(point common.Point) (expiredNum int) {
	// first delete point expired
	now := time.Now().Unix()
	var index int = 0
	for i, p := range cpb.Data {
		if p.Timestamp >= now - cpb.duration {
			break
		}
		index = i
	}
	cpb.Lock()
	
	if index > 0 {
		cpb.Data = cpb.Data[index:]
	}
	expiredNum = index
	last := len(cpb.Data) -1
	
	// push point at right location, maybe some point reach delay
	cpb.Data = append(cpb.Data, point)
	for i := last; i >0; i-- {
		if cpb.Data[i].Timestamp < point.Timestamp {
			break
		}
		cpb.Data[i+1] = cpb.Data[i]
		cpb.Data[i] = point
	}
	
	if cpb.PointsToDb == nil {
		cpb.PointsToDb = make([]common.Point, 0)
	}
	cpb.PointsToDb = append(cpb.PointsToDb, point)
	cpb.Unlock()
	return
}

//
func (cpb *CachePointBag) GetPointBagForDb() *common.PointBag{
	cpb.Lock()
	defer cpb.Unlock()
	
	pb := common.NewPointsBag(cpb.Metric)
	if cpb.PointsToDb == nil {
		return pb
	}
	
	for _, p := range cpb.PointsToDb {
		pb.Append(p)
	}
	cpb.PointsToDb = nil
	return pb
}

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type Cache struct {
	SizeLimit  int64  // limit when add pointBag ,if the pointBag data size over this limit, drop it
	writeStrategy WriteStrategy
	data          []*Shard
	ChanForDB     chan *common.PointBag
	
	size          int64
	
	logger        *log.Vlogger
	stat          *statsd.BaseStat
}

// A "thread" safe string to anything map.
type Shard struct {
	sync.RWMutex     // Read Write mutex, guards access to internal map.
	items            map[string]*CachePointBag
	notConfirmed     []*common.PointBag  // linear search for value/slot
	notConfirmedUsed int                 // search value in notConfirmed[:notConfirmedUsed]
}

// Creates a new cache instance
func New(sizeLimit int64) *Cache {
	c := &Cache{
		writeStrategy: TimeSortedStrategy,
		SizeLimit:     sizeLimit, // default 1M
		data:          make([]*Shard, shardCount),
		ChanForDB:     make(chan *common.PointBag, 1024*1024), // 1M  // TODO set from config
		stat:          common.GetStat("cache"),
		logger:        log.GetLogger("cache", log.RotateMode16M),
	}

	for i := 0; i < shardCount; i++ {
		c.data[i] = &Shard{
			items:        make(map[string]*CachePointBag),
			notConfirmed: make([]*common.PointBag, 0),
		}
	}

	return c
}

// Return shard under given key
func (c *Cache) GetShard(key string) *Shard {
	// @TODO: remove type casts ?
	hash_key := uint(common.Hash_fnv32(key)) % uint(shardCount)
	return c.data[hash_key]
}

func (c *Cache) Get(key string, from, util int64) (bool, []common.Point) {
	c.stat.CounterInc("query-times", 1)
	shard := c.GetShard(key)
	var data []common.Point = make([]common.Point, 0)
	
	shard.Lock()
	defer shard.Unlock()
	if p, exists := shard.items[key]; exists {
		if from >= time.Now().Unix() - p.duration {
			for _, dp := range p.Data {
				if dp.Timestamp >= from && dp.Timestamp <= util{
					data = append(data, dp)
				}
			}
		}else{
			return false, nil
		}
		
	}
	return true, data
}
func (c *Cache) GetMetricInfo(key string) (bool, int64, int) {
	c.stat.CounterInc("query-times", 1)
	shard := c.GetShard(key)

	shard.Lock()
	defer shard.Unlock()
	if p, exists := shard.items[key]; exists {
		return true, p.duration, len(p.Data)
	}
	return false, 0, 0
}

// Sets the given value under the specified key.
func (c *Cache) Add(p common.MetricPoint) {
	c.logger.DebugFilter( fiterCpuTotal(p.Key), "Cache add ", p)
	//if c.SizeLimit > 0 && c.Size() > c.SizeLimit {
	//	c.stat.CounterInc("overflow-count", 1)
	//	c.logger.DebugFilter( fiterCpuTotal(p.Key), "Cache add overflow", p)
	//	return
	//}
	// Get map shard.
	shard := c.GetShard(p.Key)

	shard.Lock()
	if _, exists := shard.items[p.Key]; !exists {
		shard.items[p.Key] = &CachePointBag{
			PointBag:   *common.NewPointsBag(p.Key),
			PointsToDb: make([]common.Point, 0),
			duration:   3600,
		}
	}
	expiredNum := shard.items[p.Key].Add(common.Point{p.Value, p.Timestamp})
	shard.Unlock()
	
	if expiredNum > 0 {
		atomic.AddInt64(&c.size, 0-int64(expiredNum))
	}
	
	atomic.AddInt64(&c.size, 1)
	c.stat.GaugeUpdate("point-count", atomic.LoadInt64(&c.size))
}

// SetAddSizeLimit  set limit when add point bag ,if data num of point-bag over the limit ,drop the point-bag
func (c *Cache) SetAddSizeLimit(maxSize int64) {
	c.SizeLimit = int64(maxSize)
}

// Get the num of how much point-bag in cache
func (c *Cache) Len() int32 {
	l := 0
	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.Lock()
		l += len(shard.items)
		shard.Unlock()
	}
	return int32(l)
}

// Get the num of how much point in cache, note that one point-bag has many point
func (c *Cache) Size() int64 {
	return c.size
}

func fiterCpuTotal(key string) bool {
	if strings.Index(key, "cpu.cpu-total.idle") >0 {
		return true
	}
	return false
}