package index_service

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kisaragi77/TinyES/types"
	"github.com/kisaragi77/TinyES/util"
)

const (
	INDEX_SERVICE = "index_service"
)

// IndexWorker grpc server
type IndexServiceWorker struct {
	Indexer *Indexer // foward and reverse index
	//Config of service registration
	hub      *ServiceHub
	selfAddr string
}

// Initialize index
func (service *IndexServiceWorker) Init(DocNumEstimate int, dbtype int, DataDir string) error {
	service.Indexer = new(Indexer)
	return service.Indexer.Init(DocNumEstimate, dbtype, DataDir)
}

// Register to service center
func (service *IndexServiceWorker) Regist(etcdServers []string, servicePort int) error {
	if len(etcdServers) > 0 {
		if servicePort <= 1024 {
			return fmt.Errorf("invalid listen port %d, should more than 1024", servicePort)
		}
		selfLocalIp, err := util.GetLocalIP()
		if err != nil {
			panic(err)
		}
		selfLocalIp = "127.0.0.1" //TODO: Fix selfLocalIp 127.0.0.1 at Local Testing
		service.selfAddr = selfLocalIp + ":" + strconv.Itoa(servicePort)
		var heartBeat int64 = 3
		hub := GetServiceHub(etcdServers, heartBeat)
		leaseId, err := hub.Regist(INDEX_SERVICE, service.selfAddr, 0)
		if err != nil {
			panic(err)
		}
		service.hub = hub
		go func() {
			for {
				hub.Regist(INDEX_SERVICE, service.selfAddr, leaseId)
				time.Sleep(time.Duration(heartBeat)*time.Second - 100*time.Millisecond)
			}
		}()
	}
	return nil
}

// Close index
func (service *IndexServiceWorker) Close() error {
	if service.hub != nil {
		service.hub.UnRegist(INDEX_SERVICE, service.selfAddr)
	}
	return service.Indexer.Close()
}

// Delete Documnet from index RPC
func (service *IndexServiceWorker) DeleteDoc(ctx context.Context, docId *DocId) (*AffectedCount, error) {
	return &AffectedCount{int32(service.Indexer.DeleteDoc(docId.DocId))}, nil
}

// Add/Update Documnet to index RPC.
func (service *IndexServiceWorker) AddDoc(ctx context.Context, doc *types.Document) (*AffectedCount, error) {
	n, err := service.Indexer.AddDoc(*doc)
	return &AffectedCount{int32(n)}, err
}

// Search index RPC
func (service *IndexServiceWorker) Search(ctx context.Context, request *SearchRequest) (*SearchResult, error) {
	result := service.Indexer.Search(request.Query, request.OnFlag, request.OffFlag, request.OrFlags)
	return &SearchResult{Results: result}, nil
}

// Index Count RPC
func (service *IndexServiceWorker) Count(ctx context.Context, request *CountRequest) (*AffectedCount, error) {
	return &AffectedCount{int32(service.Indexer.Count())}, nil
}
