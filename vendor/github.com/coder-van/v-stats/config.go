package statsd

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
)

type Config struct {
	IsLocal                bool   `toml:"is_local"`
	ReceiverAddr           string `toml:"receiver_addr"`
	ReceiverQueueSize      int    `toml:"receiver_queue_size"`
	GraphiteAddr           string `toml:"graphite_addr"`
	DataPointQueueSize     int    `toml:"datapoint_queue_size"`
	BackendFlushSeconds    int    `toml:"backend_flush_seconds"`
	BackendFlushSize       int    `toml:"backend_flush_size"`
	AggregatorFlushSeconds int    `toml:"aggregator_flush_seconds"`
}

func NewConfig() *Config {
	return &Config{
		IsLocal:                true,
		ReceiverAddr:           ":8125",
		ReceiverQueueSize:      100000,
		GraphiteAddr:           ":2003",
		DataPointQueueSize:     100000,
		BackendFlushSeconds:    5,
		BackendFlushSize:       64,
		AggregatorFlushSeconds: 5,
	}
}

func (c *Config) LoadConfig(confPath string) (*Config, error) {

	if confPath, err := getDefaultConfigPath(confPath); err != nil {
		return nil, err
	} else {
		fmt.Printf("Loading config file: %s \n", confPath)
		if _, err := toml.DecodeFile(confPath, c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func getDefaultConfigPath(cp string) (string, error) {
	/*
	 Try to find a default config file at current or /etc dir
	*/

	confName := cp
	etcConfPath := "/etc/" + cp
	return getPath(etcConfPath, confName)
}

func getPath(paths ...string) (string, error) {
	fmt.Println("Search config file in paths:", paths)
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	// if we got here, we didn't find a file in a default location
	return "", fmt.Errorf("Could not find path in %s", paths)
}
