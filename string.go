package main

import "strings"

func StripRedisUrl(redisPath string) []string {
	redisUrlList := []string{}

	redisList := strings.Split(redisPath, "|")
	for i := 0; i < len(redisList); i++ {
		redisPath := strings.Split(redisList[i], ":")
		redisHost := redisPath[0]
		redisPortList := strings.Split(redisPath[1], ",")
		for j := 0; j < len(redisPortList); j++ {
			redisUrl := redisHost + ":" + redisPortList[j]
			redisUrlList = append(redisUrlList, redisUrl)
		}
	}
	return redisUrlList
}
