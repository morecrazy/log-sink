package main
import (
	"third/redigo/redis"
	"fmt"
)

func consumer(redisUrl string) {
	c, err := redis.Dial("tcp", redisUrl)
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
	defer c.Close()
	defer wg.Done()

	var channel = make(chan []byte, gWriterCount)
	for i := 0; i < int(gWriterCount); i++ {
		go writer(channel)
	}
	for {
		ele, err := redis.Strings(c.Do("BLPOP", gRedisKey,"5"))
		if err != nil {
			fmt.Println("redis get failed:", err)
			continue
		}
		bts := []byte(ele[1])
		channel <- bts
	}
}
