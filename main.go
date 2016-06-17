package main

import (
	"fmt"
	"backend/common"
	"flag"
	"runtime"
	"sync"
)

var mapLogNameToLogFile map[string]*File = make(map[string]*File)
var mapLogNameToLogBuffer map[string]*LogBuffer = make(map[string]*LogBuffer)
var wg sync.WaitGroup

const (
	DEFAULT_CONF_FILE = "./log-sink.conf"
)

var g_conf_file string
var gRedisPath string
var gRedisKey string
var gChannelBufferSize int64
var gBufferWriterNum int64
var gLogSize int64
var gLogUnit string
var gLogBufferSize int64

func init() {
	const usage = "log-sink [-c config_file]"
	flag.StringVar(&g_conf_file, "c", "", usage)
}

func InitExternalConfig(config *common.Configure)  {
	gRedisPath = config.External["redisPath"]
	gRedisKey = config.External["redisKey"]
	gLogUnit = config.External["logUnit"]
	gLogSize = config.ExternalInt64["logSize"]
	gChannelBufferSize = config.ExternalInt64["channelBufferSize"]
	gBufferWriterNum = config.ExternalInt64["bufferWriterNum"]
	gLogBufferSize = config.ExternalInt64["logBufferSize"]
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

	redisUrlList := StripRedisUrl(gRedisPath)
	wg.Add(len(redisUrlList))

	for i := 0; i < len(redisUrlList); i++ {
		common.Logger.Debug("the redis url is %s: ", redisUrlList[i])
		go consumer(redisUrlList[i])
	}

	fmt.Println("Sink log service is started...")
	wg.Wait()
}
