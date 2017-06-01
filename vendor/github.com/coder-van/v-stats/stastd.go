package statsd

import (
	"github.com/coder-van/v-stats/backends"
	"github.com/coder-van/v-stats/metrics"
	"github.com/coder-van/v-stats/receivers"
	"github.com/coder-van/v-util/log"
)

func NewStatsD(conf *Config) *StatsD {
	if conf == nil {
		conf = NewConfig()
	}
	ch1 := make(chan []byte, conf.ReceiverQueueSize)
	ch2 := make(chan metrics.MetricDataPoint, conf.DataPointQueueSize)
	return &StatsD{
		config:           NewConfig(),
		PacketInChannel:  ch1,
		dataPointChannel: ch2,
		logger:           log.GetLogger("statsd", log.RotateModeMonth),
		backendManger:    backends.NewBackendManger(conf.BackendFlushSeconds, ch2, conf.BackendFlushSize),
		receiver:         receivers.NewUdpReceiver(conf.ReceiverAddr, ch1),
	}
}

type StatsD struct {
	config           *Config
	metricRegistry   metrics.Registry
	PacketInChannel  chan []byte                  // Channel for all incoming statsd packets
	dataPointChannel chan metrics.MetricDataPoint // channel for backends to read
	logger           *log.Vlogger
	agg              *aggregator
	receiver         receivers.Receiver
	backendManger    *backends.BackendManger
}

func (s *StatsD) ResetConfig(c *Config) {
	s.config = c
	panic("has not implement")
}

func (s *StatsD) LoadConfig(fp string) {
	s.config.LoadConfig(fp)
}

func (s *StatsD) SetRegistry(registry metrics.Registry) {
	s.metricRegistry = registry
	s.agg = NewAggregator(s.config.AggregatorFlushSeconds, s.metricRegistry, s.PacketInChannel, s.dataPointChannel)
}

func (s *StatsD) StartAll() {
	s.logger.Println("Statsd starting")
	// first start aggregator
	s.agg.Start()

	// then start backend manager
	s.backendManger.RegisterGraphite(s.config.GraphiteAddr)
	s.backendManger.Start()

	// last start receiver
	if !s.config.IsLocal {
		s.receiver.Start()
	}
	s.logger.Println("statsd started ")
}

func (s *StatsD) StopAll() {
	s.logger.Println("Statsd stoping")

	if !s.config.IsLocal {
		s.receiver.Stop()
	}
	s.agg.Stop()
	s.backendManger.Stop()

	close(s.PacketInChannel)
	close(s.dataPointChannel)
	s.logger.Println("statsd stoped ")
}
