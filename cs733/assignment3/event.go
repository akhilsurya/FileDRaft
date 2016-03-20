package main

type AppendEv struct {
	data []byte
}

type VoteReqEv struct {
	CandidateId    int
	Term           int
	RecentLogIndex int
	RecentLogTerm  int
}

type TimeoutEv struct {
}

type AppendEntriesReqEv struct {
	Leader       int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntriesRespEv struct {
	PeerId      int
	Term        int
	Success     bool
	MatchedTill int
}

type VoteRespEv struct {
	PeerId  int
	Term    int
	Success bool
}
