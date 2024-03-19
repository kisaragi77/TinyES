package index_service

import types "github.com/kisaragi77/TinyES/types"

// Interface Of Indexer (Can be implemented by Sentinel and Indexer)
type IIndexer interface {
	AddDoc(doc types.Document) (int, error)
	DeleteDoc(docId string) int
	Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []*types.Document
	Count() int
	Close() error
}
