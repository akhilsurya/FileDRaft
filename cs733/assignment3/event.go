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
	leader       int
	term         int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	commitIndex  int
}

type AppendEntriesRespEv struct {
	peerId      int
	term        int
	success     bool
	matchedTill int
}

type VoteRespEv struct {
	peerId  int
	term    int
	success bool
}
