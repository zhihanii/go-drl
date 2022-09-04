package raft

import "time"

type Config struct {
	LocalId            string
	LocalAddr          string
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	NotifyCh           chan bool
	Configuration      Configuration
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout: 1000 * time.Millisecond,
		ElectionTimeout:  1000 * time.Millisecond,
	}
}
