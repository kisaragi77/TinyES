package index_service

import (
	"bytes"
	"encoding/gob"
	"strings"
	"sync/atomic"

	"github.com/kisaragi77/TinyES/internal/kvdb"
	reverseindex "github.com/kisaragi77/TinyES/internal/reverse_index"
	"github.com/kisaragi77/TinyES/types"
	"github.com/kisaragi77/TinyES/util"
)

// Combine forward and reverse index
type Indexer struct {
	forwardIndex kvdb.IKeyValueDB
	reverseIndex reverseindex.IReverseIndexer
	maxIntId     uint64
}

// Initialize the index
func (indexer *Indexer) Init(DocNumEstimate int, dbtype int, DataDir string) error {
	db, err := kvdb.GetKvDb(dbtype, DataDir)
	if err != nil {
		return err
	}
	indexer.forwardIndex = db
	indexer.reverseIndex = reverseindex.NewSkipListReverseIndex(DocNumEstimate)
	return nil
}

// Load data from index file when system restarts
func (indexer *Indexer) LoadFromIndexFile() int {
	reader := bytes.NewReader([]byte{})
	n := indexer.forwardIndex.IterDB(func(k, v []byte) error {
		reader.Reset(v)
		decoder := gob.NewDecoder(reader)
		var doc types.Document
		err := decoder.Decode(&doc)
		if err != nil {
			util.Log.Printf("gob decode document failed：%s", err)
			return nil
		}
		indexer.reverseIndex.Add(doc)
		return err
	})
	util.Log.Printf("load %d data from forward index %s", n, indexer.forwardIndex.GetDbPath())
	return int(n)
}

// Close index
func (indexer *Indexer) Close() error {
	return indexer.forwardIndex.Close()
}

// Add/Upsert document to index. If exists, delete first.
func (indexer *Indexer) AddDoc(doc types.Document) (int, error) {
	docId := strings.TrimSpace(doc.Id)
	if len(docId) == 0 {
		return 0, nil
	}
	indexer.DeleteDoc(docId)

	doc.IntId = atomic.AddUint64(&indexer.maxIntId, 1)
	var value bytes.Buffer
	encoder := gob.NewEncoder(&value)
	if err := encoder.Encode(doc); err == nil {
		indexer.forwardIndex.Set([]byte(docId), value.Bytes())
	} else {
		return 0, err
	}

	indexer.reverseIndex.Add(doc)
	return 1, nil
}

// Delete document from index
func (indexer *Indexer) DeleteDoc(docId string) int {
	n := 0
	forwardKey := []byte(docId)
	docBs, err := indexer.forwardIndex.Get(forwardKey)
	if err == nil {
		reader := bytes.NewReader([]byte{})
		if len(docBs) > 0 {
			n = 1
			reader.Reset(docBs)
			decoder := gob.NewDecoder(reader)
			var doc types.Document
			err := decoder.Decode(&doc)
			if err == nil {
				for _, kw := range doc.Keywords {
					indexer.reverseIndex.Delete(doc.IntId, kw)
				}
			}
		}
	}
	indexer.forwardIndex.Delete(forwardKey)
	return n
}

// Return  list of documents by searching the query from index
func (indexer *Indexer) Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []*types.Document {
	docIds := indexer.reverseIndex.Search(query, onFlag, offFlag, orFlags)
	if len(docIds) == 0 {
		return nil
	}
	keys := make([][]byte, 0, len(docIds))
	for _, docId := range docIds {
		keys = append(keys, []byte(docId))
	}
	docs, err := indexer.forwardIndex.BatchGet(keys)
	if err != nil {
		util.Log.Printf("read kvdb failed: %s", err)
		return nil
	}
	result := make([]*types.Document, 0, len(docs))
	reader := bytes.NewReader([]byte{})
	for _, docBs := range docs {
		if len(docBs) > 0 {
			reader.Reset(docBs)
			decoder := gob.NewDecoder(reader)
			var doc types.Document
			err := decoder.Decode(&doc)
			if err == nil {
				result = append(result, &doc)
			}
		}
	}
	return result
}

// Return number of documents in index
func (indexer *Indexer) Count() int {
	n := 0
	indexer.forwardIndex.IterKey(func(k []byte) error {
		n++
		return nil
	})
	return n
}
