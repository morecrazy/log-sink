package main

import (
	"fmt"
	"os"
	"backend/common"
	"flag"
	"runtime"
)

var mapLogNameToLogFile map[string]*os.File = make(map[string]*os.File)
var channel = make(chan []byte, gWorkerCount)

const (
	DEFAULT_CONF_FILE = "./log-sink.conf"
)

var g_conf_file string
var gRedisPath string
var gRedisPort string
var gRedisListKey string
var gWorkerCount int64


func init() {
	const usage = "log-sink [-c config_file]"
	flag.StringVar(&g_conf_file, "c", "", usage)
}

func InitExternalConfig(config *common.Configure)  {
	gRedisPath = config.External["redisPath"]
	gRedisPort = config.External["redisPort"]
	gRedisListKey = config.External["redisListKey"]
	gWorkerCount = config.ExternalInt64["workerCount"]
}

func main() {
	common.Logger.Info("starting collect log\n")
	//set runtime variable
	runtime.GOMAXPROCS(runtime.NumCPU())
	//get flag
	flag.Parse()

	if g_conf_file != "" {
		common.Config = new(common.Configure)
		if err := common.InitConfigFile(g_conf_file, common.Config); err != nil {
			fmt.Println("init config err : ", err)
		}
	} else {
		addrs := []string{"http://etcd.in.codoon.com:2379"}
		common.Config = new(common.Configure)
		if err := common.LoadCfgFromEtcd(addrs, "log-sink", common.Config); err != nil {
			fmt.Println("init config from etcd err : ", err)
		}
	}

	var err error
	common.Logger, err = common.InitLogger("log-sink")
	if err != nil {
		fmt.Println("init log error")
		return
	}
	InitExternalConfig(common.Config)
	for i := 0; i < int(gWorkerCount); i++ {
		go worker()
	}
	go producter()
}
