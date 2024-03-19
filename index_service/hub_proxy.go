package index_service

import (
	context "context"
	"strings"
	"sync"
	"time"

	"github.com/kisaragi77/TinyES/util"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
)

type IServiceHub interface {
	Regist(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) // Register service
	UnRegist(service string, endpoint string) error                                         // Unregister service
	GetServiceEndpoints(service string) []string                                            // Service discovery
	GetServiceEndpoint(service string) string                                               // Choose an endpoint of a service
	Close()                                                                                 // Close etcd client connection
}

// Provide cache and rate limiting protection by ServiceHub Proxy
type HubProxy struct {
	*ServiceHub
	endpointCache sync.Map
	limiter       *rate.Limiter
}

var (
	proxy     *HubProxy
	proxyOnce sync.Once
)

// Constructor of HubProxy(Return Single Instance)
//
// qps : Max requests per second
func GetServiceHubProxy(etcdServers []string, heartbeatFrequency int64, qps int) *HubProxy {
	if proxy == nil {
		proxyOnce.Do(func() {
			serviceHub := GetServiceHub(etcdServers, heartbeatFrequency)
			if serviceHub != nil {
				proxy = &HubProxy{
					ServiceHub:    serviceHub,
					endpointCache: sync.Map{},
					limiter:       rate.NewLimiter(rate.Every(time.Duration(1e9/qps)*time.Nanosecond), qps),
				}
			}
		})
	}
	return proxy
}

// Watch etcd service endpoints
func (proxy *HubProxy) watchEndpointsOfService(service string) {
	if _, exists := proxy.watched.LoadOrStore(service, true); exists {
		return
	}
	ctx := context.Background()
	prefix := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/"
	ch := proxy.client.Watch(ctx, prefix, etcdv3.WithPrefix())
	util.Log.Printf("监听服务%s的节点变化", service)
	go func() {
		for response := range ch {
			for _, event := range response.Events {
				util.Log.Printf("etcd event type %s", event.Type)
				path := strings.Split(string(event.Kv.Key), "/")
				if len(path) > 2 {
					service := path[len(path)-2]
					endpoints := proxy.ServiceHub.GetServiceEndpoints(service)
					if len(endpoints) > 0 {
						proxy.endpointCache.Store(service, endpoints)
					} else {
						proxy.endpointCache.Delete(service)
					}
				}
			}
		}
	}()
}

// Service discovery
//
// Update cache when etcd changes.
func (proxy *HubProxy) GetServiceEndpoints(service string) []string {

	if !proxy.limiter.Allow() {
		return nil
	}

	proxy.watchEndpointsOfService(service)
	if endpoints, exists := proxy.endpointCache.Load(service); exists {
		return endpoints.([]string)
	} else {
		endpoints := proxy.ServiceHub.GetServiceEndpoints(service)
		if len(endpoints) > 0 {
			proxy.endpointCache.Store(service, endpoints)
		}
		return endpoints
	}
}
