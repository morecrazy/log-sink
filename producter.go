package main
import (
	"third/redigo/redis"
	"fmt"
)

func producter() {
	var redisUrl = gRedisPath + gRedisPort
	c, err := redis.Dial("tcp", redisUrl)
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
	defer c.Close()

	for {
		ele, err := redis.Strings(c.Do("BLPOP", gRedisListKey,"5"))
		if err != nil {
			fmt.Println("redis get failed:", err)
			continue
		}
		bts := []byte(ele[1])
		channel <- bts
	}
}
