package backends

import (
	gb "github.com/coder-van/v-stats/backends/graghite"
	"github.com/coder-van/v-stats/metrics"
	"github.com/coder-van/v-util/log"
	"strings"
	"sync"
	"time"
)

type InterfaceBackend interface {
	Flush(batch []byte) error
}

func NewBackendManger(seconds int,
	dataPointCh chan metrics.MetricDataPoint, bufSize int) *BackendManger {

	b := &BackendManger{
		exit:               make(chan bool),
		RegisteredBackends: make(map[string]InterfaceBackend, 10), // todo size
		metricsBuffer:      NewBuffer(bufSize * 2),
		metricsBufferSize:  bufSize * 2,
		FlushInterval:      time.Duration(1e9 * seconds),
		dataPointCh:        dataPointCh,
		logger:             log.GetLogger("statsd", log.RotateModeMonth),
	}
	return b
}

type BackendManger struct {
	exit               chan bool
	RegisteredBackends map[string]InterfaceBackend
	metricsBuffer      *Buffer
	metricsBufferSize  int
	FlushInterval      time.Duration
	dataPointCh        chan metrics.MetricDataPoint
	logger             *log.Vlogger
}

func (b *BackendManger) RegisterBackend(name string, backend InterfaceBackend) {
	if _, ok := b.RegisteredBackends[name]; ok {
		return
	}
	b.RegisteredBackends[name] = backend
}

func (b *BackendManger) RegisterGraphite(addr string) {
	// address, _ := net.ResolveTCPAddr("net", addr)
	g := gb.NewGraphite(addr)
	b.RegisterBackend("graghite:"+addr, g)
}

func (b *BackendManger) run(shutdown chan bool, interval time.Duration) {
	defer close(b.exit)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	b.logger.Println("Statsd BackendManger started")
	for {
		select {
		case <-shutdown:
			b.logger.Println("Hang on, Flush before shutdown")
			b.Flush()
			b.logger.Println("Statsd BackendManger stoped")
			return
		case <-ticker.C:
			b.Flush()
		case m := <-b.dataPointCh:
			b.add(m)
		}
	}
}

func (b *BackendManger) Start() {
	b.logger.Println("Statsd BackendManger starting")
	go b.run(b.exit, b.FlushInterval)
}

func (b *BackendManger) Stop() {
	b.logger.Println("Statsd BackendManger stoping")
	b.exit <- true
}

func (b *BackendManger) add(dp metrics.MetricDataPoint) {
	// fmt.Printf("add datapoint %s", dp.String())
	d := dp.String()
	if strings.Index(d, "cpu.cpu-total.idle") >= 0 {
		b.logger.Println(d)
	}
	b.metricsBuffer.Add(dp)
	// 当缓存到了50%的时候就刷新缓存了
	if b.metricsBuffer.Len() >= int(b.metricsBufferSize) {
		b.Flush()
	}
}

func (b *BackendManger) Flush() {
	var wg sync.WaitGroup
	batch := b.metricsBuffer.Batch(b.metricsBufferSize)

	wg.Add(len(b.RegisteredBackends))
	for _, bm := range b.RegisteredBackends {
		go func() {
			defer wg.Done()
			err := bm.Flush(batch)
			if err != nil {
				b.logger.Printf("Error occurred when posting to Forwarder API: %s \n", err.Error())
			}

		}()
	}

	wg.Wait()
}
