package main

import (
	"flag"
	"net"
	"os"
	"net/http"
	"strings"
	"io"
	"github.com/parnurzeal/gorequest"
	"github.com/robfig/cron"
	"time"
	"fmt"
	"sync"
	"encoding/json"
)

var (
	Name           = "kafka_exporter"
	listenAddress  = flag.String("unix-sock", "/dev/shm/kafka_exporter.sock", "Address to listen on for unix sock access and telemetry.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	url            = flag.String("url", "http://localhost:9999", "KafkaOffsetMonitor url.")
)

var g_lock sync.RWMutex
var g_ret string
var g_doing bool

type Broker struct {
	Id int
	Host string
	Port int
}

type Offset struct {
	Group string
	Topic string
	Partition int
	Offset int64
	LogSize int64
	Owner string
	Creation int64
	Modified int64
}

type OffsetInfo struct {
	Brokers []Broker
	Offsets []Offset
}

type Ch struct {
	Name string
	Children []interface{}
}

type Cluster struct {
	Name string
	Children []Ch
}

func metrics(w http.ResponseWriter, req *http.Request) {
	g_lock.RLock()
	io.WriteString(w, g_ret)
	g_lock.RUnlock()
}

func doWork() {
	if g_doing {
		return
	}
	g_doing = true

	ret := ""
	nameSpace := "kafka"

	// 1. get kafka groups
	// ["a","b","c"]
	req := gorequest.New()
	_, body, errs := req.Retry(1, 2 * time.Second,
		http.StatusBadRequest, http.StatusInternalServerError).
			Get(fmt.Sprintf("%s/group", *url)).End()
	if errs != nil {
		g_lock.Lock()
		g_ret = ret
		g_lock.Unlock()
		g_doing = false
		return
	}
	if strings.HasPrefix(body,"[") && strings.HasSuffix(body,"]") {
		body = strings.Replace(body,"[", "",-1)
		body = strings.Replace(body,"]", "",-1)
		body = strings.Replace(body,"\"", "",-1)
	}
	grps := strings.Split(body, ",")
	if len(grps) > 0 {
		// 2. get offset
		for _, i := range grps {
			req := gorequest.New()
			_, body, errs := req.Retry(1, 2 * time.Second,
				http.StatusBadRequest, http.StatusInternalServerError).
					Get(fmt.Sprintf("%s/group/%s", *url, i)).End()
			if errs != nil {
				continue
			}
			info := &OffsetInfo{}
			err := json.Unmarshal([]byte(body), info)
			if err != nil {
				continue
			}
			for _, ofs := range info.Offsets {
				ret = ret + fmt.Sprintf("%s_offset{group=\"%s\",topic=\"%s\",partition=\"%d\"} %g\n",
					nameSpace, ofs.Group, ofs.Topic, ofs.Partition, float64(ofs.Offset))
				ret = ret + fmt.Sprintf("%s_log_size{group=\"%s\",topic=\"%s\",partition=\"%d\"} %g\n",
					nameSpace, ofs.Group, ofs.Topic, ofs.Partition, float64(ofs.LogSize))
				ret = ret + fmt.Sprintf("%s_lag{group=\"%s\",topic=\"%s\",partition=\"%d\"} %g\n",
					nameSpace, ofs.Group, ofs.Topic, ofs.Partition, float64(ofs.LogSize) - float64(ofs.Offset))
			}
		}
	}
	// 3. cluster list
	req2 := gorequest.New()
	_, body2, errs := req2.Retry(1, 2 * time.Second,
		http.StatusBadRequest, http.StatusInternalServerError).
		Get(fmt.Sprintf("%s/clusterlist", *url)).End()
	if errs == nil {
		cl := &Cluster{}
		err := json.Unmarshal([]byte(body2), cl)
		if err == nil {
			for _, ch := range cl.Children {
				l := strings.Split(ch.Name, ":")
				if len(l) == 2 {
					ret = ret + fmt.Sprintf("%s_node{addr=\"%s\",kafakaip=\"%s\",port=\"%s\"} 1\n",
						nameSpace, ch.Name, l[0], l[1])
				}
			}
		}
	}

	g_lock.Lock()
	g_ret = ret
	g_lock.Unlock()
	g_doing = false
}

func main() {
	flag.Parse()
	if url == nil {
		panic("error url")
	}

	g_doing = false
	doWork()
	c := cron.New()
	c.AddFunc("0 */2 * * * ?", doWork)
	c.Start()

	mux := http.NewServeMux()
	mux.HandleFunc(*metricsPath, metrics)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>Kafka Exporter</title></head>
             <body>
             <h1>Kafka Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	server := http.Server{
		Handler: mux, // http.DefaultServeMux,
	}
	os.Remove(*listenAddress)

	listener, err := net.Listen("unix", *listenAddress)
	if err != nil {
		panic(err)
	}
	server.Serve(listener)
}