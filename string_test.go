package main

import "testing"
import "fmt"

func TestStripRedisUrl(t *testing.T)  {
	testRedisUrlList := StripRedisUrl("10.117.26.250:6379,6380,6381,6382,6383")

	for _, item := range testRedisUrlList {
		fmt.Println(item)
	}

	testRedisUrlList = StripRedisUrl("10.117.26.250:6379,6380,6381,6382,6383|10.117.26.250:6379,6380,6381,6382,6383")

	for _, item := range testRedisUrlList {
		fmt.Println(item)
	}
}
