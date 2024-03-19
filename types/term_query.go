package types

import (
	"strings"
)

func NewTermQuery(field, keyword string) *TermQuery {
	return &TermQuery{Keyword: &Keyword{Field: field, Word: keyword}}
}

func (q TermQuery) Empty() bool {
	return q.Keyword == nil && len(q.Must) == 0 && len(q.Should) == 0
}

// Return a new TermQuery with the given querys as must
func (q *TermQuery) And(querys ...*TermQuery) *TermQuery {
	if len(querys) == 0 {
		return q
	}
	array := make([]*TermQuery, 0, 1+len(querys))
	// Skip empty query
	if !q.Empty() {
		array = append(array, q)
	}
	for _, ele := range querys {
		if !ele.Empty() {
			array = append(array, ele)
		}
	}
	return &TermQuery{Must: array} //Only Must is not empty
}

func (q *TermQuery) Or(querys ...*TermQuery) *TermQuery {
	if len(querys) == 0 {
		return q
	}
	array := make([]*TermQuery, 0, 1+len(querys))
	if !q.Empty() {
		array = append(array, q)
	}
	for _, ele := range querys {
		if !ele.Empty() {
			array = append(array, ele)
		}
	}
	return &TermQuery{Should: array} //Only Should is not empty
}

func (q TermQuery) ToString() string {
	if q.Keyword != nil {
		return q.Keyword.ToString()
	} else if len(q.Must) > 0 {
		if len(q.Must) == 1 {
			return q.Must[0].ToString()
		} else {
			sb := strings.Builder{}
			sb.WriteByte('(')
			for _, e := range q.Must {
				s := e.ToString()
				if len(s) > 0 {
					sb.WriteString(s)
					sb.WriteByte('&')
				}
			}
			s := sb.String()
			s = s[0:len(s)-1] + ")"
			return s
		}
	} else if len(q.Should) > 0 {
		if len(q.Should) == 1 {
			return q.Should[0].ToString()
		} else {
			sb := strings.Builder{}
			sb.WriteByte('(')
			for _, e := range q.Should {
				s := e.ToString()
				if len(s) > 0 {
					sb.WriteString(s)
					sb.WriteByte('|')
				}
			}
			s := sb.String()
			s = s[0:len(s)-1] + ")"
			return s
		}

	}
	return ""
}
