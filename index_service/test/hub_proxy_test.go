package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/kisaragi77/TinyES/index_service"
)

func TestGetServiceEndpointsByProxy(t *testing.T) {
	const qps = 10
	proxy := index_service.GetServiceHubProxy(etcdServers, 3, qps)

	endpoint := "127.0.0.1:5000"
	proxy.Regist(serviceName, endpoint, 0)
	defer proxy.UnRegist(serviceName, endpoint)
	endpoints := proxy.GetServiceEndpoints(serviceName)
	fmt.Printf("endpoints %v\n", endpoints)

	endpoint = "127.0.0.2:5000"
	proxy.Regist(serviceName, endpoint, 0)
	defer proxy.UnRegist(serviceName, endpoint)
	endpoints = proxy.GetServiceEndpoints(serviceName)
	fmt.Printf("endpoints %v\n", endpoints)

	endpoint = "127.0.0.3:5000"
	proxy.Regist(serviceName, endpoint, 0)
	defer proxy.UnRegist(serviceName, endpoint)
	endpoints = proxy.GetServiceEndpoints(serviceName)
	fmt.Printf("endpoints %v\n", endpoints)

	time.Sleep(1 * time.Second)
	for i := 0; i < qps+5; i++ {
		endpoints = proxy.GetServiceEndpoints(serviceName)
		fmt.Printf("%d endpoints %v\n", i, endpoints)
	}

	time.Sleep(1 * time.Second)
	for i := 0; i < qps+5; i++ {
		endpoints = proxy.GetServiceEndpoints(serviceName)
		fmt.Printf("%d endpoints %v\n", i, endpoints)
	}
}

// go test -v ./index_service/test -run=^TestGetServiceEndpointsByProxy$ -count=1
