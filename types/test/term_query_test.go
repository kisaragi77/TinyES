package test

import (
	"fmt"
	"testing"

	"github.com/kisaragi77/TinyES/types"
)

const FIELD = ""

// ((A|B|C)&D)|E&((F|G)&H)

func TestTermQuery(t *testing.T) {
	A := types.NewTermQuery(FIELD, "")
	B := types.NewTermQuery(FIELD, "B")
	C := types.NewTermQuery(FIELD, "C")
	D := types.NewTermQuery(FIELD, "D")
	E := &types.TermQuery{}
	F := types.NewTermQuery(FIELD, "F")
	G := types.NewTermQuery(FIELD, "G")
	H := types.NewTermQuery(FIELD, "H")

	var q *types.TermQuery

	q = A
	fmt.Println(q.ToString())

	q = B.Or(C)
	fmt.Println(q.ToString())

	// ((A|B|C)&D)|E&((F|G)&H)
	q = A.Or(B).Or(C).And(D).Or(E).And(F.Or(G)).And(H)
	fmt.Println(q.ToString())
}

// go test -v ./types/test -run=^TestTermQuery$ -count=1
