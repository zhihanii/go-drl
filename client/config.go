package client

import "time"

type ServerInfo struct {
	Id string
	Addr string
}

type Configuration struct {
	Servers []ServerInfo
}

type Config struct {
	TakeInterval time.Duration
	LeaderId     string
	LeaderAddr   string
	Configuration Configuration
}
