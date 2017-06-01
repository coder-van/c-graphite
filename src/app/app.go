package app

import (
	"runtime"

	"github.com/coder-van/v-carbon/src/cache"
	"github.com/coder-van/v-carbon/src/persists"
	"github.com/coder-van/v-carbon/src/receivers"
	"github.com/coder-van/v-util/log"

	"fmt"
	"os"
	"strings"
	"time"
	"github.com/coder-van/v-carbon/src/common"
)

type App struct {
	// sync.RWMutex
	Name            string
	ConfigDir       string
	Config          *Config
	Cache           *cache.Cache
	ReceiverManager *receivers.ReceiverManager
	PersistManager  *persists.PersistManager
	apiServer       *ApiServer
}

// New App instance
func New(confDir string) *App {
	app := &App{
		Name:      "Carbon",
		ConfigDir: confDir,
	}
	return app
}

// configure loads config from config file, schemas.conf, aggregation.conf
func (app *App) Init() error {
	cfg, err := LoadConfig(app.ConfigDir)
	if err != nil {
		return err
	}

	app.Config = cfg
	app.ConfigDir = cfg.Dir

	log.SetLogDir(cfg.Logging.LogRoot)
	log.Debug = app.Config.Debug
	
	core := cache.New(cfg.Cache.MaxSize)
	core.SetWriteStrategy(cfg.Cache.WriteStrategy)
	app.Cache = core

	app.ReceiverManager = receivers.New()
	for name, r := range app.Config.Receivers {
		listen := r.Listen
		t := strings.SplitN(listen, ":", 2)[0]
		port := strings.SplitN(listen, ":", 2)[1]
		receiver, err := app.ReceiverManager.CreateNewReceiver(name, t, port, r.IsPickle)
		if err != nil {
			return err
		}
		app.ReceiverManager.RegisterReceiver(receiver)
	}
	app.ReceiverManager.CachePB = core.Add
	
	if app.Config.Persist.DataRoot != "" {
		_, err := os.Stat(app.Config.Persist.DataRoot)
		if err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

// Start starts
func (app *App) Start() (err error) {
	app.Init()
	conf := app.Config

	runtime.GOMAXPROCS(conf.Common.MaxCPU)

	if conf.Cache.DumpEnable {
		app.Cache.RestoreAll(conf.Cache.DumpPath)
	}

	app.PersistManager = persists.NewPersistManager(app.Cache, time.Millisecond*200.0)
	app.PersistManager.RegisterWhisper(app.Config.Persist.DataRoot, app.ConfigDir)
	
	app.PersistManager.Start()
	app.ReceiverManager.Start()

	app.apiServer = NewApiServer(app.PersistManager, app.Cache)
	app.apiServer.Start()
	
	stat := NewStat(app.Cache, 5)
	stat.Start()
	return
}

func (app *App) Stop() {
	fmt.Println("* app stopping")
	if app.Config.Cache.DumpEnable {
		app.Cache.Dump(app.Config.Cache.DumpPath, false)
	}
	if app.ReceiverManager != nil {
		app.ReceiverManager.Stop()
	}

	if app.PersistManager != nil {
		app.PersistManager.Stop()
	}
	
	fmt.Println("* app stopped")
}

func NewStat(c *cache.Cache, secs time.Duration) *Stat {
	return &Stat{
		FlushSecs:     secs,
		exit:          make(chan bool),
		cache:         c,
	}
}

type Stat struct {
	FlushSecs time.Duration
	cache     *cache.Cache
	exit      chan bool
}

func (s *Stat) Run() {
	ticker := time.NewTicker( time.Duration(s.FlushSecs)*time.Second)
	fmt.Println("* Stat started")
	for {
		select {
		case <-ticker.C:
			// fmt.Println("stat flush", time.Now())
			common.Flush(s.cache.Add, s.FlushSecs)
		case <-s.exit:
			fmt.Println("* Stat stopped")
			return
		}
	}
}

func (s *Stat) Start() {
	fmt.Println("* Stat starting")
	go s.Run()
}

func (s *Stat) Stop() {
	fmt.Println("* Stat stopping")
	if s.exit != nil {
		s.exit <- true
	}
}