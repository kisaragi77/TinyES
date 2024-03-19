package test

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/kisaragi77/TinyES/index_service"
	"github.com/kisaragi77/TinyES/internal/kvdb"
	"github.com/kisaragi77/TinyES/types"
	"github.com/kisaragi77/TinyES/util"
	"google.golang.org/grpc"
)

var (
	workPorts   = []int{5678, 5679, 5660}
	etcdServers = []string{"127.0.0.1:2379"}
	workers     []*index_service.IndexServiceWorker
)

func StartWorkers() {
	workers = make([]*index_service.IndexServiceWorker, 0, len(workPorts))
	for i, port := range workPorts {
		lis, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			panic(err)
		}

		server := grpc.NewServer()
		service := new(index_service.IndexServiceWorker)
		service.Init(50000, kvdb.BADGER, util.RootPath+"data/local_db/book_badger_"+strconv.Itoa(i))
		service.Indexer.LoadFromIndexFile()
		index_service.RegisterIndexServiceServer(server, service)
		service.Regist(etcdServers, port)
		go func(port int) {
			fmt.Printf("start grpc server on port %d\n", port)
			err = server.Serve(lis)
			if err != nil {
				service.Close()
				fmt.Printf("start grpc server on port %d failed: %s\n", port, err)
			} else {
				workers = append(workers, service)
			}
		}(port)
	}
}

func StopWorkers() {
	for _, worker := range workers {
		worker.Close()
	}
}

func TestIndexCluster(t *testing.T) {
	StartWorkers()
	time.Sleep(3 * time.Second)
	defer StopWorkers()

	sentinel := index_service.NewSentinel(etcdServers)
	book := Book{
		ISBN:    "436246383",
		Title:   "上下五千年",
		Author:  "李四",
		Price:   39.0,
		Content: "冰雪奇缘2 中文版电影原声带 (Frozen 2 (Mandarin Original Motion Picture",
	}
	doc := types.Document{
		Id:          book.ISBN,
		BitsFeature: 0b10011,
		Keywords:    []*types.Keyword{{Field: "content", Word: "唐朝"}, {Field: "content", Word: "文物"}, {Field: "title", Word: book.Title}},
		Bytes:       book.Serialize(),
	}
	n, err := sentinel.AddDoc(doc)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	} else {
		fmt.Printf("添加%d个doc\n", n)
	}
	query := types.NewTermQuery("content", "文物")
	query = query.And(types.NewTermQuery("content", "唐朝"))
	docs := sentinel.Search(query, 0, 0, nil)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	} else {
		docId := ""
		if len(docs) == 0 {
			fmt.Println("无搜索结果")
		} else {
			for _, doc := range docs {
				book := DeserializeBook(doc.Bytes)
				if book != nil {
					fmt.Printf("%s %s %s %s %.1f\n", doc.Id, book.ISBN, book.Title, book.Author, book.Price)
					docId = doc.Id
				}
			}
		}
		if len(docId) > 0 {
			n := sentinel.DeleteDoc(docId)
			fmt.Printf("删除%d个doc\n", n)
		}

		docs := sentinel.Search(query, 0, 0, nil)
		if len(docs) == 0 {
			fmt.Println("无搜索结果")
		} else {
			for _, doc := range docs {
				book := DeserializeBook(doc.Bytes)
				if book != nil {
					fmt.Printf("%s %s %s %s %.1f\n", doc.Id, book.ISBN, book.Title, book.Author, book.Price)
				}
			}
		}
	}
}

// go test -v ./index_service/test -run=^TestIndexCluster$ -count=1
