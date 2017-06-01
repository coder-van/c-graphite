package app

import (
	"fmt"
	"github.com/coder-van/v-carbon/src/persists"
	"github.com/coder-van/v-util/log"
	"gopkg.in/gin-gonic/gin.v1"
	"math"
	"regexp"
	"strconv"
	"time"
	"github.com/coder-van/v-carbon/src/cache"
	"github.com/coder-van/v-carbon/src/common"
	"github.com/coder-van/v-stats"
	"path"
	"github.com/coder-van/v-stats/metrics"
)


type fetchResponse struct {
	data           []byte
	contentType    string
	metricsFetched int
	valuesFetched  int
	memoryUsed     int
}

type ApiServer struct {
	cache  *cache.Cache
	pm     *persists.PersistManager
	logger *log.Vlogger
	stat          *statsd.BaseStat
}

func NewApiServer(pm *persists.PersistManager, c *cache.Cache) *ApiServer {
	return &ApiServer{
		cache:  c,
		pm:     pm,
		logger: log.GetLogger("api", log.RotateModeMonth),
		stat:   common.GetStat("api"),
	}
}

func (api *ApiServer) listHandler(c *gin.Context) {
	// URL: /metrics/list/?format=json
	api.stat.GaugeInc("list-requests", 1)
	metrics := api.pm.FindMetricList()

	c.JSON(200, metrics)
	return

}

func (api *ApiServer) findHandler(c *gin.Context) {
	// URL: /metrics/find/?local=1&format=pickle&query=the.metric.path.with.glob
	
	api.stat.GaugeInc("find-requests", 1)
	query := c.DefaultQuery("query", "")

	if query == "" {
		api.stat.OnErr("error-find-request-param-empty",
			fmt.Errorf("%s", "param query can not empty"))
		c.JSON(400, gin.H{
			"error": "param query can not empty",
		})
	}

	nodes, err := api.pm.FindNodes(query)
	if err != nil {
		api.stat.OnErr("error-find-request--find-node-fail",
			fmt.Errorf("can't find nodes about %s", query))
		c.JSON(400, gin.H{
			"error": "can't find nodes " + query,
		})
	}

	c.Header("Access-Control-Allow-Origin", "*")
	c.JSON(200, nodes)
}

type Datapoint interface{}

type RenderTarget struct {
	Target     string      `json:"target"`
	Datapoints []Datapoint `json:"datapoints"`
}

func NewRenderTarget(target string) *RenderTarget {
	return &RenderTarget{
		Target:     target,
		Datapoints: make([]Datapoint, 0),
	}
}

const (
	Seconds = 1
	Minutes = 60
	Hours   = 3600
	Days    = 86400
	Weeks   = Days * 7
	Months  = Days * 30
	Years   = Days * 365
)

func convertTime(t string) (int64, error) {
	now := time.Now().Unix()
	if t == "now" {
		return time.Now().Unix(), nil
	}
	var timeRegexp *regexp.Regexp = regexp.MustCompile("^(-?\\d+)(s|min|h|d|w|mon|y)+$")
	if !timeRegexp.MatchString(t) {
		value, err := strconv.ParseInt(t, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("Can't convert param %s to timestamp", t)
		}
		return value, nil
	} else {
		matches := timeRegexp.FindStringSubmatch(t)
		value, err := strconv.ParseInt(matches[1], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("Can't convert param %s to timestamp", t)
		}
		s := matches[2]
		switch s {
		case "s":
			return now + value*Seconds, nil
		case "min":
			return now + value*Minutes, nil
		case "h":
			return now + value*Hours, nil
		case "d":
			return now + value*Days, nil
		case "w":
			return now + value*Weeks, nil
		case "mon":
			return now + value*Months, nil
		case "y":
			return now + value*Years, nil
		}
	}

	return 0, nil
}

type RenderResponse []*RenderTarget

func (api *ApiServer) renderHandler(c *gin.Context) {
	api.stat.GaugeInc("render-requests", 1)
	targets := c.PostFormArray("target")
	from := c.DefaultPostForm("from", "-3h")
	until := c.DefaultPostForm("until", "now")

	from_timestamp, err := convertTime(from)
	until_timestamp, err := convertTime(until)
	
	if err != nil {
		c.JSON(400, gin.H{
			"error": err,
		})
	}
	
	renderResponse := make([]*RenderTarget, 0)
	for _, target := range targets {
		api.stat.GaugeInc("render-requests", 1)
		wfs, err := api.pm.DbInstance.Match(target)
		if err != nil {
			api.stat.OnErr("error-render-request-target-match", err)
			c.JSON(400, gin.H{
				"error": err,
			})
		}
		
		for _, k := range wfs {
			api.stat.GaugeInc("render-requests-target", 1)
			// 先查看缓存
			isOk, points := api.cache.Get(k, from_timestamp, until_timestamp)
			rt := NewRenderTarget(k)
			
			if isOk {
				// 缓存命中
				api.stat.GaugeInc("render-requests-from-cache", 1)
				for _, p := range points {
					dp := make([]interface{}, 2)
					if math.IsNaN(p.Value) {
						dp[0] = 0
					} else {
						dp[0] = p.Value
					}
					
					dp[1] = p.Timestamp
					rt.Datapoints = append(rt.Datapoints, dp)
				}
				
			}else{
				api.stat.GaugeInc("render-requests-from-db", 1)
				wf, err := api.pm.DbInstance.Open(k)
				if err != nil {
					api.stat.OnErr("error-render-request-wf-open", err)
					continue
				}
				timeSeries, err := wf.Fetch(int(from_timestamp), int(until_timestamp))
				if err != nil {
					api.stat.OnErr("error-render-request-wf-fetch", err)
					wf.Close()
					continue
				}
				
				for _, p := range timeSeries.Points() {
					dp := make([]interface{}, 2)
					if math.IsNaN(p.Value) {
						dp[0] = 0
					} else {
						dp[0] = p.Value
					}
					
					dp[1] = p.Time
					rt.Datapoints = append(rt.Datapoints, dp)
				}
				wf.Close()
				
			}
			renderResponse = append(renderResponse, rt)
		}
	}
	c.Header("Access-Control-Allow-Origin", "*")
	c.JSON(200, renderResponse)
	return
}

func (api *ApiServer) cacheHandler(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	
	target := c.Query("target")
	interval_str := c.DefaultQuery("interval", "0")
	interval, err:= strconv.ParseInt(interval_str, 10, 64)
	if err != nil {
		c.JSON(400, gin.H{
			"error": err,
		})
	}
	now := time.Now().Unix()
	if ok, du, l := api.cache.GetMetricInfo(target); ok {
		if ok {
			if interval > du {
				c.JSON(200, "interval out cache")
			}else {
				ok, dps := api.cache.Get(target, now-interval, now)
				
				if ok {
					c.JSON(200, gin.H {
						"datapoints": dps,
						"count": l,
						"duration": du,
					} )
				}else {
					c.JSON(200, "interval out cache")
				}
			}
		}
	}else {
		c.JSON(200, "empty from cache")
	}
	
	return
}

func (api *ApiServer) statHandler(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	target := c.Query("target")
	
	datapoints := make([]Datapoint, 0)
	common.Registry.Each(func(name string, i interface{}) {
		if target != "" && target != name { return }
		switch metric := i.(type) {
		case metrics.Counter:
			dp := []interface{}{name+".Countor",metric.Count()}
			datapoints = append(datapoints, dp)
		case metrics.Gauge:
			dp := []interface{}{name+".Gauge",metric.Value()}
			datapoints = append(datapoints, dp)
		case metrics.GaugeFloat64:
			dp := []interface{}{name+".GaugeFloat64",metric.Value()}
			datapoints = append(datapoints, dp)
		}
	})
	c.JSON(200, datapoints)
	return
}

func (api *ApiServer) Stop() {

}

func (api *ApiServer) Start() {
	go api.RegisterRouter(":8080")
}

func (api *ApiServer) RegisterRouter(addr string) {
	gin.SetMode(gin.ReleaseMode)
	log_path := path.Join(path.Base(api.logger.FilePath), "api-server.log")
	gin.LoggerWithWriter(log.NewDailyRotateHandler(log_path, 7))
	router := gin.Default()
	router.GET("/metrics/find/", api.findHandler)
	router.OPTIONS("/metrics/find/", api.findHandler)
	router.GET("/metrics/list/", api.listHandler)
	router.POST("/render", api.renderHandler)
	router.GET("/cache/", api.cacheHandler)
	router.GET("/status/", api.statHandler)
	router.GET("/", func(c *gin.Context) {
		c.JSON(200, "ok")
	})
	router.Run(addr)
	
}
