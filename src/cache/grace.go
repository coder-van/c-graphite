package cache

import (
	"bufio"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"fmt"
	// "github.com/coder-van/v-carbon/src/common"
	"github.com/coder-van/v-util/log"
	"io"
	"github.com/coder-van/v-graphite/src/common"
)

const (
	isDumpInBinary = false
)

type SyncWriter struct {
	sync.Mutex
	w *bufio.Writer
}

func (s *SyncWriter) Write(p []byte) (n int, err error) {
	s.Lock()
	n, err = s.w.Write(p)
	s.Unlock()
	return
}

func (s *SyncWriter) Flush() error {
	return s.w.Flush()
}

// Dump
func (c *Cache) Dump(dumpPath string, isDumpInBinary bool) {

	logger := log.GetLogger("dump_restore", log.RotateModeMonth)
	logger.Println("---------------------- Dump start ---------------------")

	cacheSize := c.Size()
	if cacheSize == 0 {
		logger.Println("no points need to dump")
	}
	filenamePostfix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	dumpFilename := path.Join(dumpPath, fmt.Sprintf("cache.%s.bin", filenamePostfix))
	
	dump, err := os.Create(dumpFilename)
	if err != nil {
		logger.Printf("dump create file error, %s \n", err)
	}
	dumpWriter := bufio.NewWriterSize(dump, 1024*1024) // 1Mb

	// dump cache
	if ! isDumpInBinary {
		c.DumpInStr(dumpWriter)
	}
	
	if err != nil {
		logger.Printf("dump write fail, %s \n", err)
	}

	if err = dumpWriter.Flush() ; err != nil {
		logger.Printf("dump flush fail, %s \n", err)
	}

	if err = dump.Close(); err != nil {
		logger.Printf("dumped but fail to close writer, %s \n", err)
	}

	logger.Printf("cache dump finished points count: %d \n", int(cacheSize))
	logger.Println("--------------------- Dump finished --------------------")
}

// TODO
func (c *Cache) DumpInStr(w io.Writer) error {
	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.Lock()
		
		for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
			if p == nil {
				continue
			}
			if _, err := WriteTo(p, w); err != nil {
				shard.Unlock()
				return err
			}
		}
		
		for _, p := range shard.items {
			if _, err := WriteTo(&p.PointBag, w); err != nil {
				shard.Unlock()
				return err
			}
		}
		
		shard.Unlock()
	}
	
	return nil
}


// RestoreFromFile read and parse data from single file
func (c *Cache) RestoreFromFile(filename string) {
	var pointsCount int
	startTime := time.Now()

	logger := log.GetLogger("dump_restore", log.RotateModeMonth)
	logger.Printf("restoring from %s \n", filename)
	

	err := ReadFrom(filename, func(p common.MetricPoint) {
		pointsCount += 1
		c.Add(p)
	})
	
	if err != nil {
		logger.Printf("error in RestoreFromFile, %s \n", err)
		panic("RestoreFromFile error")
	}

	logger.Printf("restore %d points, use %s s from file: %s finished",
			pointsCount, time.Since(startTime), filename)
	
	err = os.Remove(filename)
	if err != nil {
		logger.Error("failed to remove file",  filename, zap.Error(err))
		panic("failed to remove file " + filename)
	}
	
}

// RestoreFromDir cache and input dumps from disk to memory
func (c *Cache) RestoreAll(dumpDir string) {
	startTime := time.Now()

	logger := log.GetLogger("dump_restore", log.RotateModeMonth)
	logger.Println("------------------------ dump restore ------------------------")
	logger.Printf("restore from dir %s \n", dumpDir)
	
	files, err := ioutil.ReadDir(dumpDir)
	if err != nil {
		if strings.Index(err.Error(), "no such file or directory") > 0 {
			logger.Printf("dir %s not exist!!! \n", dumpDir)
		}else{
			logger.Error("restore failed:", zap.Error(err))
		}
		return
	}

	// read files and lazy sorting
	list := make([]string, 0)

FilesLoop:
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		r := strings.Split(file.Name(), ".")
		if len(r) < 3 {  // {input,cache}.pid.nanotimestamp(.+)?
			continue
		}

		var fileWithSortPrefix string

		switch r[0] {
		case "cache":
			fileWithSortPrefix = fmt.Sprintf("%s_%s:%s", r[2], "1", file.Name())
		case "input":
			fileWithSortPrefix = fmt.Sprintf("%s_%s:%s", r[2], "2", file.Name())
		default:
			continue FilesLoop
		}

		list = append(list, fileWithSortPrefix)
	}

	if len(list) == 0 {
		logger.Println("nothing for restore")
		return
	}

	sort.Strings(list)

	for index, fileWithSortPrefix := range list {
		list[index] = strings.SplitN(fileWithSortPrefix, ":", 2)[1]
	}

	for _, fn := range list {
		filename := path.Join(dumpDir, fn)
		c.RestoreFromFile(filename)
	}
	
	logger.Printf("restore finished use %s \n", time.Since(startTime))
	logger.Println("------------------------ restore finished ------------------------")
}

//func (c *Cache) DumpBinary(w io.Writer) error {
//	for i := 0; i < shardCount; i++ {
//		shard := c.data[i]
//		shard.Lock()
//
//		for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
//			if p == nil {
//				continue
//			}
//			if _, err := p.WriteBinaryTo(w); err != nil {
//				shard.Unlock()
//				return err
//			}
//		}
//
//		for _, p := range shard.items {
//			if _, err := p.WriteBinaryTo(w); err != nil {
//				shard.Unlock()
//				return err
//			}
//		}
//
//		shard.Unlock()
//	}
//
//	return nil
//}



