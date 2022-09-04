package client

import "sync"

type Bucket struct {
	lock sync.Mutex

	tokens int64
}

func (tb *Bucket) Take(n int64) bool {
	tb.lock.Lock()
	defer tb.lock.Unlock()
	if tb.tokens >= n {
		tb.tokens -= n
		return true
	} else {
		return false
	}
}

func (tb *Bucket) UpdateTokens(tokens int64) {
	tb.lock.Lock()
	tb.tokens = tokens
	tb.lock.Unlock()
}
