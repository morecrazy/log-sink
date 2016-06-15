package main

import (
	"fmt"
	"os"
	"backend/common"
	"flag"
	"runtime"
	"sync"
	"strings"
)

var mapLogNameToLogFile map[string]*os.File = make(map[string]*os.File)
var wg sync.WaitGroup
const (
	DEFAULT_CONF_FILE = "./log-sink.conf"
)

var g_conf_file string
var gRedisPath string
var gRedisPortList string
var gRedisKey string
var gWriterCount int64
var gLogSize int64
var gLogUnit string

func init() {
	const usage = "log-sink [-c config_file]"
	flag.StringVar(&g_conf_file, "c", "", usage)
}

func InitExternalConfig(config *common.Configure)  {
	gRedisPath = config.External["redisPath"]
	gRedisPortList = config.External["redisPortList"]
	gRedisKey = config.External["redisKey"]
	gLogUnit = config.External["logUnit"]
	gLogSize = config.ExternalInt64["logSize"]
	gWriterCount = config.ExternalInt64["writerCount"]
}

func main() {
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
	ports := strings.Split(gRedisPortList, "|")

	wg.Add(len(ports))
	for i := 0; i < len(ports); i++ {
		redisUrl := gRedisPath + ports[i]
		go consumer(redisUrl)
	}
	wg.Wait()
}
