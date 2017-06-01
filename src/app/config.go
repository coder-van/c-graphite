package app

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
	"path/filepath"
	"strconv"
)

const FileName = "carbon.conf"

type commConfig struct {
	MaxCPU int `toml:"max-cpu"`
}

func (c *commConfig) String() string {
	str := "MaxCPU int :" + string(c.MaxCPU)
	return str
}

type loggingConfig struct {
	LogRoot  string
	LogLevel string `toml:"log-level"`
}

type whisperConfig struct {
	DataRoot        string `toml:"data-dir"`
	SchemasFilename string `toml:"schemas-file"`
}

type persist struct {
	whisperConfig
}

type cacheConfig struct {
	MaxSize       int64  `toml:"max-size"`
	WriteStrategy string `toml:"write-strategy"`
	DumpPath      string `toml:"dump-path"`
	DumpEnable    bool   `toml:"dump-enable"`
}

type cacheQueryConfig struct {
	Listen   string `toml:"listen"`
	IsPickle bool   `toml:"is-pickle"`
}

type receiverConfig struct {
	Listen   string `toml:"listen"`
	IsPickle bool   `toml:"is-pickle"`
}

func (c *receiverConfig) String() string {
	str := "Listen string =" + string(c.Listen)
	str += "IsPickle  bool=" + strconv.FormatBool(c.IsPickle)
	return str
}

// Config ...
type Config struct {
	Debug      bool                      `toml:"debug"`
	Dir        string
	Common     commConfig                `toml:"util"`
	Cache      cacheConfig               `toml:"cache"`
	Persist    whisperConfig             `toml:"whisper"`
	Logging    loggingConfig             `toml:"logging"`
	Receivers  map[string]receiverConfig `toml:"receivers"`
	CacheQuery cacheQueryConfig          `toml:"cache-query"`
}

func NewConfig() *Config {
	cfg := &Config{
		Common: commConfig{
			MaxCPU: 1,
		},
		Cache: cacheConfig{
			MaxSize:       1000000,
			WriteStrategy: "max",
		},
	}

	return cfg
}

func LoadConfig(confDir string) (*Config, error) {
	cfg := NewConfig()
	cfg.Dir = confDir
	var cp string
	var err error
	if confDir == "" {
		cp, err = getDefaultConfigPath()
		if err != nil {
			return nil, err
		}
		cfg.Dir = filepath.Dir(cp)

	} else {
		cp = filepath.Join(cp, FileName)
	}
	fmt.Printf("Loading config from %s \n", cp)
	if _, err := toml.DecodeFile(cp, cfg); err != nil {
		return nil, err
	}
	cfg.Check()
	return cfg, nil
}

func (c *Config) String() (str string) {
	str = ""
	str += "Common:" + c.Common.String() + "\n"
	str += "Receivers:"
	for name, r := range c.Receivers {
		str += "\n\t" + name + "\t" + r.String()
	}
	str += "\n"
	return
}

func (c *Config) Check() {
	lp, err := getPath("/var/log/v-carbon", "./log")
	if err != nil {
		panic("Log dir set fail")
		return
	}
	if c.Cache.MaxSize < 1024 * 1024 {
		fmt.Println("warn caache max_size can't smaller than 1024*1024, set to 1M")
		c.Cache.MaxSize = 1024*1024
	}
	c.Logging.LogRoot = lp
}

func getDefaultConfigPath() (string, error) {
	/*
	 Try to find a default config file at current or /etc dir
	*/
	etcConfPath := "/etc/v-carbon/carbon.conf"
	return getPath(etcConfPath, "./conf/carbon.conf")
}

func getPath(paths ...string) (string, error) {
	fmt.Println("Search file in paths:", paths)
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	// if we got here, we didn't find a file in a default location
	return "", fmt.Errorf("Could not find path in %s", paths)
}
