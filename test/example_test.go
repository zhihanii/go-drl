package test

import (
	"fmt"
	"go-tbs/raft"
	"log"
	"sync"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	configuration := raft.Configuration{
		Servers: []raft.ServerInfo{
			{
				Suffrage: raft.Voter,
				Id:       "server1",
				Address:  "127.0.0.1:9001",
			},
			{
				Suffrage: raft.Voter,
				Id:       "server2",
				Address:  "127.0.0.1:9002",
			},
			{
				Suffrage: raft.Voter,
				Id:       "server3",
				Address:  "127.0.0.1:9003",
			},
		},
	}

	trans1, err := raft.NewNetworkTransport("127.0.0.1:9001")
	if err != nil {
		fmt.Println(err)
	}
	server1, err := raft.New(&raft.Config{
		LocalId:            "server1",
		LocalAddr:          "127.0.0.1:9001",
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}, raft.NewInmemStore(), trans1)
	if err != nil {
		fmt.Println(err)
	}

	trans2, err := raft.NewNetworkTransport("127.0.0.1:9002")
	if err != nil {
		fmt.Println(err)
	}
	server2, err := raft.New(&raft.Config{
		LocalId:            "server2",
		LocalAddr:          "127.0.0.1:9002",
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}, raft.NewInmemStore(), trans2)
	if err != nil {
		fmt.Println(err)
	}

	trans3, err := raft.NewNetworkTransport("127.0.0.1:9003")
	if err != nil {
		fmt.Println(err)
	}
	server3, err := raft.New(&raft.Config{
		LocalId:            "server3",
		LocalAddr:          "127.0.0.1:9003",
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}, raft.NewInmemStore(), trans3)
	if err != nil {
		fmt.Println(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		//defer wg.Done()
		err := server1.Bootstrap(configuration.Clone())
		if err != nil {
			fmt.Println(err)
		}
	}()

	go func() {
		//defer wg.Done()
		err := server2.Bootstrap(configuration.Clone())
		if err != nil {
			fmt.Println(err)
		}
	}()

	go func() {
		//defer wg.Done()
		err := server3.Bootstrap(configuration.Clone())
		if err != nil {
			fmt.Println(err)
		}
	}()

	select {
	case <-time.After(time.Second * 3):
		log.Println("start to add voter")
	}

	configuration1 := raft.Configuration{
		Servers: []raft.ServerInfo{
			{
				Suffrage: raft.Voter,
				Id:       "server1",
				Address:  "127.0.0.1:9001",
			},
			{
				Suffrage: raft.Voter,
				Id:       "server2",
				Address:  "127.0.0.1:9002",
			},
			{
				Suffrage: raft.Voter,
				Id:       "server3",
				Address:  "127.0.0.1:9003",
			},
			{
				Suffrage: raft.Voter,
				Id:       "server4",
				Address:  "127.0.0.1:9004",
			},
		},
	}

	trans4, err := raft.NewNetworkTransport("127.0.0.1:9004")
	if err != nil {
		log.Println(err)
	}
	server4, err := raft.New(&raft.Config{
		LocalId:            "server4",
		LocalAddr:          "127.0.0.1:9004",
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}, raft.NewInmemStore(), trans4)
	if err != nil {
		log.Println(err)
	}
	err = server4.Bootstrap(configuration1)
	if err != nil {
		log.Println(err)
	}

	future := server1.AddVoter("server4", "127.0.0.1:9004", time.Second*3)
	err = future.Error()
	log.Printf("add voter future err:%v", err)

	<-time.After(time.Second)
	c := server1.getLatestConfiguration()
	log.Printf("server1 configuration:%v", c)
	c = server2.getLatestConfiguration()
	log.Printf("server2 configuration:%v", c)

	//future = server1.RemoveServer("server4", time.Second*5)
	//err = future.Error()
	//log.Printf("remove server future err:%v", err)
	//
	//c = server1.getLatestConfiguration()
	//log.Printf("server1 configuration:%v", c)
	//c = server4.getLatestConfiguration()
	//log.Printf("server4 configuration:%v", c)

	server1.Shutdown()

	wg.Wait()
}
