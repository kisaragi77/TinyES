package test

import (
	"fmt"
	"testing"

	"github.com/kisaragi77/TinyES/util"
)

func TestGetLocalIP(t *testing.T) {
	fmt.Println(util.GetLocalIP())
}

// go test -v ./util/test -run=^TestGetLocalIP$ -count=1
