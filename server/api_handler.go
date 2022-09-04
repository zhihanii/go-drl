package server

import "go-drl/server/protocol"

func (s *Server) handleTake(req *protocol.Request, c *protocol.TakeRequest) {
	var (
		resp protocol.TakeResponse
		err  error
	)
	defer func() {
		req.Respond(&resp, err)
	}()

	resp.LeaderId, resp.LeaderAddr = s.raft.GetLeader()
	if !s.raft.IsLeader() {
		resp.Success = false
		return
	}
	resp.Tokens = s.bucket.TakeBatch()
	return
}
