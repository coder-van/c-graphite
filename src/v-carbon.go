package main

import (
	"flag"
	"syscall"

	// daemon "github.com/sevlyar/go-daemon"
	"github.com/coder-van/v-carbon/src/app"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	/*
		  cmd flags
			  help: print info of carbon
			  -d: run in background
			  -p: pid file path
			  dumpInfo: dump files info
	*/
	// help := flag.String("help", "", "get help info")
	// isDaemon := flag.Bool("d", false, "run service in background")
	// pidPath := flag.String("p", "/var/run/carbon.pid", "pid file path")
	// dumpInfo := flag.String("dumpInfo", "", "get info of dump files")

	configPath := flag.String("config", "", "config file path")
	flag.Parse()

	carbon := app.New(*configPath)
	
	
	signCh := make(chan os.Signal, 1)
	exitCh := make(chan bool, 1)
	
	go func() {
		signal.Notify(signCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
		sig := <-signCh
		log.Println(sig)
		exitCh <- true
		log.Println("Carbon stopping, need 2s")
		carbon.Stop()
	}()

	carbon.Start()
	
	<-exitCh
	time.Sleep(time.Second*time.Duration(2))
	log.Println("Carbon stopped")
}
