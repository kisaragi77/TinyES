package test

import (
	"fmt"
	"testing"

	"github.com/huandu/skiplist"
)

func TestSkipList(t *testing.T) {
	list := skiplist.New(skiplist.Int32)
	list.Set(24, 31)
	list.Set(24, 40)
	list.Set(12, 40)
	list.Set(18, 3)
	list.Remove(12)
	if value, ok := list.GetValue(18); ok {
		fmt.Println(value)
	}
	fmt.Println("------------------")
	node := list.Front()
	for node != nil {
		fmt.Println(node.Key(), node.Value)
		node = node.Next()
	}
}

// go test -v ./util/test -run=^TestSkipList$ -count=1
