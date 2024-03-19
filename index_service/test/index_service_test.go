package test

import (
	"context"
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
	"google.golang.org/grpc/credentials/insecure"
)

var (
	servicePort = 5678
)

func StartService() {
	lis, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(servicePort))
	if err != nil {
		panic(err)
	}

	server := grpc.NewServer()
	service := new(index_service.IndexServiceWorker)
	service.Init(50000, kvdb.BADGER, util.RootPath+"data/local_db/book_badger")
	service.Indexer.LoadFromIndexFile()
	index_service.RegisterIndexServiceServer(server, service)
	go func() {
		fmt.Printf("start grpc server on port %d\n", servicePort)
		err = server.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()
}

func TestIndexService(t *testing.T) {
	StartService()
	time.Sleep(1 * time.Second)

	conn, err := grpc.DialContext(
		context.Background(),
		"127.0.0.1:"+strconv.Itoa(servicePort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Printf("dial failed: %s", err)
		return
	}
	client := index_service.NewIndexServiceClient(conn)

	query := types.NewTermQuery("content", "文物")
	query = query.And(types.NewTermQuery("content", "唐朝"))
	request := &index_service.SearchRequest{
		Query: query,
	}
	result, err := client.Search(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	} else {
		docId := ""
		for _, doc := range result.Results {
			book := DeserializeBook(doc.Bytes)
			if book != nil {
				fmt.Printf("%s %s %s %s %.1f\n", doc.Id, book.ISBN, book.Title, book.Author, book.Price)
				docId = doc.Id
			}
		}
		if len(docId) > 0 {
			affect, err := client.DeleteDoc(context.Background(), &index_service.DocId{DocId: docId})
			if err != nil {
				fmt.Println(err)
				t.Fail()
			} else {
				fmt.Printf("删除%d个doc\n", affect.Count)
			}
		}
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
		affect, err := client.AddDoc(context.Background(), &doc)
		if err != nil {
			fmt.Println(err)
			t.Fail()
		} else {
			fmt.Printf("添加%d个doc\n", affect.Count)
		}
		request := &index_service.SearchRequest{
			Query: query,
		}
		result, err := client.Search(context.Background(), request)
		if err != nil {
			fmt.Println(err)
			t.Fail()
		} else {
			for _, doc := range result.Results {
				book := DeserializeBook(doc.Bytes)
				if book != nil {
					fmt.Printf("%s %s %s %s %.1f\n", doc.Id, book.ISBN, book.Title, book.Author, book.Price)
				}
			}
		}
	}
}

// go test -v ./index_service/test -run=^TestIndexService$ -count=1
