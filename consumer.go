package main
import (
	"third/redigo/redis"
	"backend/common"
)

func consumer(redisUrl string) {
	c, err := redis.Dial("tcp", redisUrl)
	if err != nil {
		common.Logger.Error("Connect to redis error: ", err)
		return
	}
	defer c.Close()
	defer wg.Done()

	var channel = make(chan []byte, gChannelBufferSize)
	for i := 0; i < int(gBufferWriterNum); i++ {
		go bufWriter(channel)
	}
	for {
		ele, err := redis.Strings(c.Do("BLPOP", gRedisKey,"5"))
		if err != nil {
			common.Logger.Error("Redis get failed: ", err)
			continue
		}
		bts := []byte(ele[1])
		channel <- bts
	}
}
