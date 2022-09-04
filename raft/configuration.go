package raft

import (
	"bufio"
	"fmt"
	"github.com/zhihanii/go-drl/pkg/bio"
)

const (
	Voter int16 = iota
	Nonvoter
)

type ServerInfo struct {
	Suffrage int16
	Id       string
	Address  string
}

func (s *ServerInfo) WriteTo(wb *bio.WriteBuffer) error {
	err := wb.WriteInt16(s.Suffrage)
	if err != nil {
		return err
	}
	err = wb.WriteString(s.Id)
	if err != nil {
		return err
	}
	return wb.WriteString(s.Address)
}

func (s *ServerInfo) ReadFrom(r *bufio.Reader) error {
	var err error
	s.Suffrage, err = bio.ReadInt16(r)
	if err != nil {
		return err
	}
	s.Id, err = bio.ReadString(r)
	if err != nil {
		return err
	}
	s.Address, err = bio.ReadString(r)
	return err
}

type Configuration struct {
	Servers []ServerInfo
}

func (c *Configuration) WriteTo(wb *bio.WriteBuffer) error {
	return wb.WriteSlice(len(c.Servers), func(i int) error {
		return c.Servers[i].WriteTo(wb)
	})
}

func (c *Configuration) ReadFrom(r *bufio.Reader) error {
	var (
		n   int32
		err error
	)
	n, err = bio.ReadInt32(r)
	if err != nil {
		return err
	}
	for i := 0; i < int(n); i++ {
		var s ServerInfo
		err = s.ReadFrom(r)
		if err != nil {
			return err
		}
		c.Servers = append(c.Servers, s)
	}
	return nil
}

func (c *Configuration) Clone() (copy Configuration) {
	copy.Servers = append(copy.Servers, c.Servers...)
	return
}

type configurations struct {
	committed Configuration
	latest    Configuration
}

func hasVote(configuration Configuration, id string) bool {
	for _, server := range configuration.Servers {
		if server.Id == id {
			return server.Suffrage == Voter
		}
	}
	return false
}

func checkConfiguration(configuration Configuration) error {
	idSet := make(map[string]bool)
	addressSet := make(map[string]bool)
	var voters int
	for _, server := range configuration.Servers {
		if server.Id == "" {
			return fmt.Errorf("empty id in configuration: %v", configuration)
		}
		if server.Address == "" {
			return fmt.Errorf("empty address in configuration: %v", server)
		}
		if idSet[server.Id] {
			return fmt.Errorf("found duplicate id in configuration: %v", server.Id)
		}
		idSet[server.Id] = true
		if addressSet[server.Address] {
			return fmt.Errorf("found duplicate address in configuration: %v", server.Address)
		}
		addressSet[server.Address] = true
		if server.Suffrage == Voter {
			voters++
		}
	}
	if voters == 0 {
		return fmt.Errorf("need at least one voter in configuration: %v", configuration)
	}
	return nil
}

func nextConfiguration(cur Configuration, change configurationChangeRequest) (Configuration, error) {
	configuration := cur.Clone()
	switch change.command {
	case AddVoter:
		newServer := ServerInfo{
			Suffrage: Voter,
			Id:       change.serverId,
			Address:  change.serverAddress,
		}
		found := false
		for i, server := range configuration.Servers {
			if server.Id == change.serverId {
				if server.Suffrage == Voter {
					configuration.Servers[i].Address = change.serverAddress
				} else {
					configuration.Servers[i] = newServer
				}
				found = true
				break
			}
		}
		if !found {
			configuration.Servers = append(configuration.Servers, newServer)
		}

	case RemoveServer:
		for i, server := range configuration.Servers {
			if server.Id == change.serverId {
				configuration.Servers = append(configuration.Servers[:i], configuration.Servers[i+1:]...)
				break
			}
		}
	}

	if err := checkConfiguration(configuration); err != nil {
		return Configuration{}, err
	}

	return configuration, nil
}
