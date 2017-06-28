package persists

import (
	"fmt"
	"github.com/coder-van/v-graphite/src/cache"
	"github.com/coder-van/v-graphite/src/common"
	whisper "github.com/coder-van/v-graphite/src/persists/whisper"
	"time"
)


func NewPersistManager(c *cache.Cache, fi time.Duration) *PersistManager {
	return &PersistManager{
		FlushInterval: fi,
		exit:          make(chan bool),
		cache:         c,
	}
}

type PersistManager struct {
	// persistInstance  InterfacePersist
	DbInstance    *whisper.Whisper
	FlushInterval time.Duration
	cache         *cache.Cache
	exit chan bool
}

func (pm *PersistManager) RegisterWhisper(rPath, cPath string) {
	pm.DbInstance = whisper.NewWhisper(rPath, cPath, pm.cache.ChanForDB)
}

func (pm *PersistManager) FindMetricList() []string {
	return pm.DbInstance.FindMetricList()
}

//type PointBagWithLock struct {
//	sync.RWMutex
//	bags []*util.PointBag
//}

func (pm *PersistManager) FindNodes(query string) (common.NodeList, error) {
	return pm.DbInstance.FindNodes(query)
}

func (pm *PersistManager) Run() {

	ticker := time.NewTicker(time.Second)
	fmt.Println("* PersistManager started")
	for {
		select {
		case <-ticker.C:
			pm.cache.MakeChanForDB()
			pm.DbInstance.Flush()
		case <-pm.exit:
			fmt.Println("* PersistManager stopped")
			return
		}
	}
}

func (pm *PersistManager) Start() {
	fmt.Println("* PersistManager starting")
	pm.DbInstance.Start()
	go pm.Run()
}

func (pm *PersistManager) Stop() {
	fmt.Println("* PersistManager stopping")
	if pm.exit != nil {
		pm.exit <- true
	}
	pm.DbInstance.Stop()
}
