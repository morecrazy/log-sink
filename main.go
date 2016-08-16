package main

import (
	"fmt"
	"backend/common"
	"flag"
	"runtime"
	"sync"
	"net/http"
	_ "net/http/pprof"
)

const (
	DEFAULT_CONF_FILE = "./log-sink.conf"
)

var (
	wg sync.WaitGroup
	mapLogNameToLogFile = make(map[string]*File)
	mapLogNameToLogBuffer = make(map[string]*LogBuffer)
	g_conf_file string
	gRedisPath string
	gRedisKey string
	gBrokers string
	gTopic string
	gChannelBufferSize int64
	gBufferWriterNum int64
	gLogSize int64
	gLogUnit string
	gLogBufferSize int64
)

func init() {
	const usage = "log-sink [-c config_file]"
	flag.StringVar(&g_conf_file, "c", "", usage)
}

func InitExternalConfig(config *common.Configure)  {
	gRedisPath = config.External["redisPath"]
	gRedisKey = config.External["redisKey"]
	gLogUnit = config.External["logUnit"]
	gBrokers = config.External["brokers"]
	gTopic = config.External["topic"]
	gLogSize = config.ExternalInt64["logSize"]
	gChannelBufferSize = config.ExternalInt64["channelBufferSize"]
	gBufferWriterNum = config.ExternalInt64["bufferWriterNum"]
	gLogBufferSize = config.ExternalInt64["logBufferSize"]
}

func startPprof() {
	go func() {
		common.Logger.Error("%v", http.ListenAndServe("localhost:6060", nil))
	}()
}

func main() {
	//set runtime variable
	runtime.GOMAXPROCS(runtime.NumCPU())
	//get flag
	flag.Parse()

	startPprof()

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
	broker := new(KafkaBroker) //注入kafka broker

	common.Logger, err = common.InitLogger("log-sink")
	if err != nil {
		fmt.Println("init log error")
		return
	}
	InitExternalConfig(common.Config)

	fmt.Println("Sink log service is started...")
	brokerList, _ := broker.GetBrokerList()

	wg.Add(1)
	go func(){
		defer wg.Done()
		err = broker.ConsumeMsg(brokerList, gTopic)
	}()
	wg.Wait()

	if err != nil {
		fmt.Println("log sin error: ", err.Error())
	}
	fmt.Println("Sink log service is over...")
}
