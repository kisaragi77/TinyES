package reverseindex

import "github.com/kisaragi77/TinyES/types"

type IReverseIndexer interface {
	Add(doc types.Document)                                                              // Add a doc to the reverse index
	Delete(IntId uint64, keyword *types.Keyword)                                         // Delete a keyword from the reverse index
	Search(q *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string // Find the query in the reverse index, return unique Id
}
