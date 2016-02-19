package main

type AppendEv struct {
	data []byte
}

type VoteReqEv struct {
	candidateId    int
	term           int
	recentLogIndex int
	recentLogTerm  int
}

type TimeoutEv struct {
}

type AppendEntriesReqEv struct {
	term         int
	leader       int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	commitIndex  int
}

type AppendEntriesRespEv struct {
	peerId  int
	term    int
	success bool
}

type VoteRespEv struct {
	peerId  int
	term    int
	success bool
}
