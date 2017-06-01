package whisper

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"bufio"
	"io"
	"path"
	"regexp"
	"sort"
	"sync"
	"time"
	
	"github.com/coder-van/v-carbon/src/common"
	"github.com/coder-van/v-util/log"
	statsd "github.com/coder-van/v-stats"
	"runtime"
)

type SynsWhisperFile struct{
	sync.Mutex
	schema Schema
	aggr *AggregationItem
	*WhisperFile
}

func (swf *SynsWhisperFile) Store(RootPath string, bag common.PointBag) {
	swf.Lock()
	defer swf.Unlock()
	
	logger := log.GetLogger("whisper", log.RotateModeMonth)
	
	p := filepath.Join(RootPath, strings.Replace(bag.Metric, ".", "/", -1)+".wsp")
	
	wf, err := Open(p)
	if err != nil {
		// create new whisper if file not exists
		
		if !os.IsNotExist(err) {
			logger.Printf("ERROR: failed to open whisper file %s, %s \n", p, err.Error())
			return
		}
		
		if err = os.MkdirAll(filepath.Dir(p), os.ModeDir|os.ModePerm); err != nil {
			logger.Printf("ERROR: mkdir failed %s, %s \n", p, err.Error())
			return
		}
		
		wf, err = Create(p, swf.schema.Retentions, swf.aggr.aggregationMethod, float32(swf.aggr.xFilesFactor))
		if err != nil {
			logger.Printf("ERROR: create new whisper file failed %s, %s \n", p, err)
			return
		}
	}
	
	swf.WhisperFile = wf
	
	l := len(bag.Data)
	points := make([]*TimeSeriesPoint, l)
	for i, r := range bag.Data {
		points[i] = &TimeSeriesPoint{Time: int(r.Timestamp), Value: r.Value}
	}
	
	defer wf.Close()
	
	defer func() {
		if r := recover(); r != nil {
			logger.Printf("Error: defer recovered UpdateMany panic %s, %s", p, fmt.Sprint(r))
		}
	}()
	
	start := time.Now()
	wf.UpdateMany(points)
	logger.Debug(fmt.Sprintf("Store %d points to %s use %s \n", l, p, time.Since(start)))
}

// Whisper manage hao whisper read and writer actions
type Whisper struct {
	mu          sync.Mutex
	schemas     WhisperSchemas
	aggregation *WhisperAggregation
	RootPath    string
	ConfigDir   string
	
	ChanForDB   chan *common.PointBag
	exit        chan bool
	wfs         map[string]*SynsWhisperFile
	
	logger      *log.Vlogger
	stat        *statsd.BaseStat
	
}

// NewWhisper create instance of Whisper
func NewWhisper(rPath, cPath string, ch chan *common.PointBag) *Whisper {
	return &Whisper{
		RootPath:  rPath,
		ConfigDir: cPath,
		
		ChanForDB: ch,
		exit:      make(chan bool),
		wfs:       make(map[string]*SynsWhisperFile),
		
		logger:    log.GetLogger("whisper", log.RotateModeMonth),
		stat:      common.GetStat("db"),
	}
}

func (w *Whisper) FindMetricList() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	metrics := make([]string, 0)
	for k := range w.wfs {
		metrics = append(metrics, k)
	}
	sort.Strings(metrics)
	return metrics
}

func (w *Whisper) FindNodes(query string) (common.NodeList, error) {

	query = strings.Replace(query, "*", ".", -1)
	pattern, err := regexp.Compile(query)
	if err != nil {
		
		return nil, err
	}
	nodes := make(common.NodeList, 0)
	for k := range w.wfs {
		if pattern.MatchString(k) {
			node := common.NewNode(k, true)
			nodes = append(nodes, node)
		}
	}
	sort.Sort(nodes)
	allNodes := w.makeBranchNodes(nodes)

	return allNodes, nil
}

func (w *Whisper) makeBranchNodes(nodes []*common.Node) common.NodeList {
	hasMap := make(map[string]bool)
	allNodes := make(common.NodeList, 0)
	for _, node := range nodes {

		if strings.Index(node.Metric, ".") < 0 {
			continue
		}
		pathDirs := strings.Split(node.Metric, ".")
		l := len(pathDirs) - 1
		baseDir := pathDirs[0]
		for i, p := range pathDirs {
			if i > 0 {
				baseDir = strings.Join([]string{baseDir, p}, ".")
			}
			if i >= l {
				break
			}
			if _, ok := hasMap[p]; !ok {
				allNodes = append(allNodes, common.NewNode(baseDir, false))
				hasMap[p] = true
			}
		}
		allNodes = append(allNodes, node)
	}
	sort.Sort(allNodes)
	return allNodes
}

func (w *Whisper) Match(query string) ([]string, error) {
	query = strings.Replace(query, "*", ".", -1)
	pattern, err := regexp.Compile(query)
	if err != nil {
		return nil, err
	}
	wfs := make([]string, 0)
	for k := range w.wfs {
		if pattern.MatchString(k) {
			wfs = append(wfs, k)
		}
	}
	return wfs, nil
}

func (w *Whisper) loadConfig() {
	w.aggregation = NewWhisperAggregation()

	agg_conf_path := filepath.Join(w.ConfigDir, "storage-aggregation.conf")

	aggregation, err := ReadAggregationConfig(agg_conf_path)
	if err != nil {
		w.logger.Error("Error on ReadAggregationConfig:", err)
	} else {
		w.aggregation = aggregation
	}

	schema_conf := filepath.Join(w.ConfigDir, "storage-schemas.conf")
	schemas, err := ReadSchemasConfig(schema_conf)

	if err != nil {
		w.logger.Fatalln("Error on ReadSchemasConfig:", err)
	}
	w.schemas = schemas
}

func (w *Whisper)  Init()  {
	w.loadConfig()
	w.loadMetricList()
}

func (w *Whisper) getSWF(metric string)  *SynsWhisperFile {
	if swf , ok := w.wfs[metric]; ok && swf != nil{
		return swf
	}else{
		w.mu.Lock()
		defer w.mu.Unlock()

		schema, ok := w.schemas.Match(metric)
		if !ok {
			fmt.Printf("no storage schema defined for metric %s", metric)
			return nil
		}

		aggr := w.aggregation.Match(metric)
		if aggr == nil {
			fmt.Printf("no storage aggregation defined for metric %s", metric)
			return nil
		}
		
		
		w.wfs[metric] =  &SynsWhisperFile{
			schema: schema,
			aggr: aggr,
		}
		w.stat.GaugeInc("metric-count", 1)
		return w.wfs[metric]
	}
}

func (w *Whisper) Run(i int)  {
	var pb *common.PointBag
	for {
		select {
		case pb = <- w.ChanForDB:
			swf := w.getSWF(pb.Metric)
			swf.Store(w.RootPath, *pb)
		case <- w.exit:
			fmt.Printf("* whisper write-goroutine %d exit \n", i)
			return
		}
	}
}

func (w *Whisper) Start() {
	w.loadConfig()
	w.loadMetricList()
	count := runtime.NumCPU()
	fmt.Printf("* whisper starting with %d goroutine \n", count)
	// how many cpu, how many goroutine
	for i :=0; i<count; i++ {
		go w.Run(i)
	}
}

func (w *Whisper) Stop() {
	count := runtime.NumCPU()
	fmt.Printf("* whisper stopping %d goroutine \n", count)
	
	for i :=0; i<count; i++ {
		w.exit <- true
	}
	w.dumpMetricList()
}

func (w *Whisper) Open(metric string) (*WhisperFile, error) {
	p := filepath.Join(w.RootPath, strings.Replace(metric, ".", "/", -1)+".wsp")

	wf, err := Open(p)
	if err != nil {
		return nil, err
	}
	
	return wf, nil
}


func (w *Whisper) Flush() {}

/* scan whisper data path, */
func (w *Whisper) WhisperFilesScan(dir string) []string {

	var files []string
	timeStart := time.Now()

	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		isWsp := strings.HasSuffix(info.Name(), ".wsp")
		if isWsp {
			files = append(files, strings.TrimPrefix(p, w.RootPath))
		}
		return nil
	})
	logger := log.GetLogger("whisper-manager", log.RotateModeMonth)
	if err != nil {
		logger.Error(err)
	}
	fileScanRuntime := time.Since(timeStart)
	l := len(files)
	logger.Printf("WhisperFilesScan get %d files use %s \n", l, fileScanRuntime)
	return files
}


func (w *Whisper) dumpMetricList() {
	dumpPath := path.Join(w.RootPath, "metirc-directory")
	file, err := os.OpenFile(dumpPath, os.O_CREATE|os.O_WRONLY, 0666)
	logger := log.GetLogger("whisper-manager", log.RotateModeMonth)
	if err != nil {
		logger.Error("Whiper dumpDirectory dump failed to open dump file,", err)
	}
	
	defer file.Close()
	
	timeStart := time.Now()
	
	for line := range w.wfs {
		file.WriteString(line + "\n")
	}
	useTime := time.Since(timeStart)
	l := len(w.wfs)
	logger.Printf("dumpDirectory write %d metric use %s \n", l, useTime)
}

func (w *Whisper) loadMetricList() {
	dumpPath := path.Join(w.RootPath, "metirc-directory")
	file, err := os.OpenFile(dumpPath, os.O_RDWR, 0666)
	logger := log.GetLogger("whisper-manager", log.RotateModeMonth)
	if err != nil {
		logger.Error("Whiper loadDirectory failed to open file, ", err)
	}
	
	defer file.Close()

	timeStart := time.Now()
	reader := bufio.NewReaderSize(file, 1024*1024)
	for {
		line, err := reader.ReadBytes('\n')

		if err != nil && err != io.EOF {
			logger.Error("Whiper loadDirectory failed ", err)
		}

		if len(line) == 0 {
			break
		}

		if line[len(line)-1] != '\n' {
			logger.Error("Whiper loadDirectory error : unfinished line in file")
		}

		s := strings.Trim(string(line), "\n \t\r")
		w.wfs[s] = w.getSWF(s)
	}
	use := time.Since(timeStart)
	l := len(w.wfs)
	
	w.stat.GaugeUpdate("metric-count", l)
	logger.Printf("loadDirectory read %d metric use %s \n", l, use)
}
