package main

import (
	"fmt"
	"backend/common"
	"codoon_ops/log-sink/cache"
	"flag"
	"runtime"
	"sync"
	"net/http"
	_ "net/http/pprof"
	"time"
)

const (
	DEFAULT_CONF_FILE = "./log-sink.conf"
)

var (
	wg sync.WaitGroup
	mapLogNameToLogFile = make(map[string]*File)

	//cache
	c = cache.New(60*time.Minute, 10*time.Minute)

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

func closeLogBuffer(name string, value interface{}) {
	common.Logger.Info("Shutdown logBuffer: %s", name)
	close(value.(*LogBuffer).closing)
}

func main() {
	//set runtime variable
	runtime.GOMAXPROCS(runtime.NumCPU())
	//get flag
	flag.Parse()

	//加入pprof
	startPprof()

	//初始化配置
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

	//启动服务
	fmt.Println("Sink log service is started...")
	broker := new(KafkaBroker) //注入kafka broker
	brokerList, _ := broker.GetBrokerList()
	c.OnEvicted(closeLogBuffer)

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
