package index_service

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/kisaragi77/TinyES/util"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

const (
	SERVICE_ROOT_PATH = "/tinyes/index" //Prefix of etcd key
)

// Service Registry Center
type ServiceHub struct {
	client             *etcdv3.Client //etcd client
	heartbeatFrequency int64          //Time Interval of Renewal Lease
	watched            sync.Map       //Watched Services
	loadBalancer       LoadBalancer   // Strategy of Load Balancing (RoundRobin/RandomSelect)
}

var (
	serviceHub *ServiceHub // Single Instance of ServiceHub (Private)
	hubOnce    sync.Once   //(Using sync.Once to ensure thread safety)
)

// Constructor of ServiceHub (Return serviceHub Public)
func GetServiceHub(etcdServers []string, heartbeatFrequency int64) *ServiceHub {
	if serviceHub == nil {
		hubOnce.Do(func() {
			if client, err := etcdv3.New(
				etcdv3.Config{
					Endpoints:   etcdServers,
					DialTimeout: 3 * time.Second,
				},
			); err != nil {
				util.Log.Fatalf("Can't connect to etcd server: %v", err) //When log.Fatal occurs, the go process will exit directly
			} else {
				serviceHub = &ServiceHub{
					client:             client,
					heartbeatFrequency: heartbeatFrequency, // The Validity Period of Lease
					loadBalancer:       &RoundRobin{},
				}
			}
		})
	}
	return serviceHub
}

// Register Service. The first time you register, write a key to etcd, and then renew lease
//
// service : Name of service
//
// endpoint : Address of service
//
// leaseID : LeaseID of service, Initial value is 0
func (hub *ServiceHub) Regist(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) {
	ctx := context.Background()
	if leaseID <= 0 {
		if lease, err := hub.client.Grant(ctx, hub.heartbeatFrequency); err != nil {
			util.Log.Printf("Failed to Create Lease: %v", err)
			return 0, err
		} else {
			key := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/" + endpoint
			if _, err = hub.client.Put(ctx, key, "", etcdv3.WithLease(lease.ID)); err != nil {
				util.Log.Printf("Cannot Write to Service %s At Node %s: %v", service, endpoint, err)
				return lease.ID, err
			} else {
				return lease.ID, nil
			}
		}
	} else {
		if _, err := hub.client.KeepAliveOnce(ctx, leaseID); err == rpctypes.ErrLeaseNotFound {
			return hub.Regist(service, endpoint, 0)
		} else if err != nil {
			util.Log.Printf("Renew Lease Failed: %v", err)
			return 0, err
		} else {
			return leaseID, nil
		}
	}
}

// Unregister Service
func (hub *ServiceHub) UnRegist(service string, endpoint string) error {
	ctx := context.Background()
	key := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/" + endpoint
	if _, err := hub.client.Delete(ctx, key); err != nil {
		util.Log.Printf("Failed to Unregister Service %s At Node %s: %v", service, endpoint, err)
		return err
	} else {
		util.Log.Printf("Unregistered Service %s At Node %s", service, endpoint)
		return nil
	}
}

// Service Discovery.
//
// The client queries etcd to obtain a set of servers, and then selects a server before RPC calls.
func (hub *ServiceHub) GetServiceEndpoints(service string) []string {
	ctx := context.Background()
	prefix := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/"
	if resp, err := hub.client.Get(ctx, prefix, etcdv3.WithPrefix()); err != nil { //Get all nodes under the prefix
		util.Log.Printf("Failed to Get Nodes of Service %s: %v", service, err)
		return nil
	} else {
		endpoints := make([]string, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			path := strings.Split(string(kv.Key), "/")
			endpoints = append(endpoints, path[len(path)-1])
		}
		util.Log.Printf("Refresh Service %s Server -- %v\n", service, endpoints)
		return endpoints
	}
}

// Select a serveraccording to load balancing
func (hub *ServiceHub) GetServiceEndpoint(service string) string {
	return hub.loadBalancer.Take(hub.GetServiceEndpoints(service))
}

// Close etcd client connection
func (hub *ServiceHub) Close() {
	hub.client.Close()
}
