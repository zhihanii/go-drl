package server

import "github.com/zhihanii/go-drl/raft"

type BucketConfiguration struct {
	Rate     float64
	Capacity int64
}

type Config struct {
	Addr                string
	RaftConfig          *raft.Config
	BucketConfiguration *BucketConfiguration
}
