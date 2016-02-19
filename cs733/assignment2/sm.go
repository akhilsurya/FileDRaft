package main

import (
	"math"
	"math/rand"
)

type LogEntry struct {
	term    int
	command []byte
}

type StateMachine struct {
	id          int
	peers       []int
	term        int
	votedFor    int // -1 means voted for no one
	state       int // 1 for follower, 2 for candidate, 3 for leader
	log         []LogEntry
	nextIndex   map[int]int
	matchIndex map[int]int
	commitIndex int
	timeout     int64
	voteCount   int
}

func random(min, max int64) int64 {
	return rand.Intn(max-min) + min
}

func (sm *StateMachine) propogate() []interface{} {
	var responses []interface{}
	for peer := range sm.peers {
		if peer != sm.id {
			prevIndex := sm.nextIndex[peer] - 1
			event := AppendEntriesReqEv{sm.id, sm.term, prevIndex, sm.log[prevIndex].term, sm.log[prevIndex+1 : len(sm.log)], sm.commitIndex}
			append(responses, Send{peer, event})
		}
	}
	return responses
}

func (sm *StateMachine) requestVote() []interface{} {
	var responses []interface{}
	for peer := range sm.peers {
		if peer != sm.id {
			event := VoteReqEv{sm.id, sm.term, len(sm.log) - 1, sm.log[len(sm.log)-1].term}
			append(peer, Send{peer, event})
		}
	}
	return responses
}

func (sm *StateMachine) sendHeartBeat() []interface{} {
	var responses []interface{}
	for peer := range sm.peers {
		if peer != sm.id {
			prevIndex := sm.nextIndex[peer] - 1
			var emptyContent []LogEntry
			event := AppendEntriesReqEv{sm.id, sm.term, prevIndex, sm.log[prevIndex].term, emptyContent, sm.commitIndex}
			append(peer, Send{peer, event})
		}
	}
	return responses
}

func (sm *StateMachine) checkAndUpdateLog(prevLogIndex, prevLogTerm, commitIndex, leader int, entries []LogEntry) []interface{} {
	var responses []interface{}
	if prevLogIndex > len(sm.log)-1 || sm.log[prevLogIndex].term != prevLogTerm {
		append(responses, Send{leader, AppendEntriesRespEv{sm.id, sm.term, false}})
	} else {
		sm.log = sm.log[0 : prevLogIndex+1]
		append(sm.log, entries...)
		for i := range entries {
			append(responses, LogStore{prevLogIndex + 1 + i, entries[i]})
		}
		sm.commitIndex = math.Max(sm.commitIndex, commitIndex)
		append(responses, Send{leader, AppendEntriesRespEv{sm.id, sm.term, true}})
	}
	return responses
}



func (sm *StateMachine) checkForCommit() {
	var responses []interface{}
	oldCommitIndex := sm.commitIndex
	curIndex := len(sm.log)-1

	for {
		count := 0
		var majority int
		majority = len(sm.peers)/2+1
		for peer := range sm.peers {
			if sm.matchIndex[peer] >= curIndex {
				count++
			}
		}
		if count >= majority && sm.log[curIndex] == sm.term {
			sm.commitIndex =curIndex
			for i:= oldCommitIndex+1; i<= curIndex; i++ {
				append(responses, Commit{i, sm.log[i].command})
			}
			break;
		} else {
			if curIndex == oldCommitIndex {
				return  responses
			} else {
				curIndex--
			}
		}
	}
	return responses
}



func (sm *StateMachine) handleAppend(ev AppendEv) []interface{} {
	var responses []interface{}
	switch sm.state {
	case 1:
		append(responses, Commit{0, ev.data, "INCORRECT_HOST"})
	case 2:
		append(responses, Commit{0, ev.data, "INCORRECT_HOST"})
	case 3:
		newEntry := LogEntry{sm.term, ev.data}
		append(sm.log, newEntry)
		append(responses, LogStore{len(sm.log), ev.data})
		append(responses, sm.propogate()...)
	}
	return responses
}

func (sm *StateMachine) handleTimeout(ev TimeoutEv) []interface{} {
	var responses []interface{}
	switch sm.state {
	case 1:
		append(responses, StateStore{sm.term + 1, sm.id})
		sm.term++
		sm.votedFor = sm.id
		sm.voteCount = 1
		sm.state = 2
		append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
		append(responses, sm.requestVote()...)
	case 2:
		append(responses, StateStore{sm.term + 1, sm.id})
		sm.term++
		sm.votedFor = sm.id
		sm.voteCount = 1
		sm.state = 2
		append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
		append(responses, sm.requestVote()...)
	case 3:
		// TODO : change timeout appropriately
		append(responses, Alarm{sm.timeout})
		append(responses, sm.sendHeartBeat()...)
	}
	return responses
}


func (sm *StateMachine) handleAppendEntriesReq(ev AppendEntriesReqEv) []interface{} {
	var responses []interface{}
	switch sm.state {
	case 1:
		if ev.term < sm.term {
			append(responses, Send{ev.leader, AppendEntriesRespEv{sm.id, sm.term, false}})
			return responses
		} else if ev.term > sm.term {
			sm.term = ev.term
			append(responses, StateStore{ev.term, sm.votedFor})
		}
		append(responses, Alarm{sm.timeout}) // TODO : Correct ?
		if ev.prevLogIndex > len(sm.log)-1 || sm.log[ev.prevLogIndex].term != ev.prevLogTerm {
			append(responses, Send{ev.leader, AppendEntriesRespEv{sm.id, sm.term, false}})
		} else {
			sm.log = sm.log[0 : ev.prevLogIndex+1]
			append(sm.log, ev.entries...)
			for i := range ev.entries {
				append(responses, LogStore{ev.prevLogIndex + 1 + i, ev.entries[i]})
			}
			sm.commitIndex = math.Max(sm.commitIndex, ev.commitIndex)
			append(responses, Send{ev.leader, AppendEntriesRespEv{sm.id, sm.term, true}})
		}
	case 2:
		if ev.term < sm.term {
			append(responses, Send{ev.leader, AppendEntriesRespEv{sm.id, sm.term, false}})
			return responses
		} else {
			if ev.term > sm.term {
				sm.term = ev.term
				sm.votedFor = -1
				append(responses, StateStore{ev.term, -1})
			}
			sm.voteCount = 0
			append(responses, Alarm{sm.timeout}) // TODO : Correct ?
			append(responses, sm.checkAndUpdateLog(ev.prevLogIndex, ev.prevLogTerm, ev.commitIndex, ev.leader, ev.entries)...)
			sm.state = 1
			// move to candidate state
		}
	case 3:
		// TODO : Think of equality case again
		if ev.term > sm.term {
			sm.voteCount = 0
			sm.term = ev.term
			sm.votedFor = -1
			append(responses, StateStore{ev.term, -1})
			append(responses, Alarm{sm.timeout}) //TODO : Correct ?
			append(responses, sm.checkAndUpdateLog(ev.prevLogIndex, ev.prevLogTerm, ev.commitIndex, ev.leader, ev.entries)...)
			sm.state = 1
			append(responses, Send{ev.leader, AppendEntriesRespEv{sm.id, sm.term, true}})
		} else {
			append(responses, Send{ev.leader, AppendEntriesRespEv{sm.id, sm.term, false}})
		}
	}
	return responses
}

func (sm *StateMachine) handleAppendEntriesResp(ev AppendEntriesRespEv) []interface{} {
	var responses []interface{}
	switch sm.state {
	case 1:
		if ev.term > sm.term {
			sm.term = ev.term
			sm.votedFor = -1
			append(responses, StateStore{sm.term, -1})
		}
	case 2:
		if ev.term > sm.term {
			sm.term = ev.term
			sm.votedFor = -1
			append(responses, StateStore{sm.term, -1})
		}
	case 3:
		if ev.term < sm.term {
			// empty
			return responses
		}
		if ev.term < sm.term {
			sm.term = ev.term
			sm.votedFor = -1
			append(responses, StateStore{sm.term, -1})
			sm.state = 1
			sm.voteCount = 1
			return responses
		}

		if ev.success {
			sm.matchIndex[ev.peerId] = sm.nextIndex[ev.peerId] // TODO : Discuss
			sm.nextIndex[ev.peerId]++
			append(responses, sm.checkForCommit()...)
		} else {
			var index int
			if sm.nextIndex[ev.peerId] == 0 {
				index = 0
			} else {
				sm.nextIndex[ev.peerId]--
				index = sm.nextIndex[ev.peerId]
			}
			append(responses, Send{ev.peerId, AppendEntriesReqEv{sm.id, sm.term, index, sm.log[index].term, sm.log[index:len(sm.log)]}})
		}
	}
	return responses
}

func (sm *StateMachine) handleVoteReq(ev VoteReqEv) []interface{} {
	var responses []interface{}
	switch sm.state {
	case 1:
		if sm.term > ev.term {
			append(responses, Send{ev.candidateId, VoteRespEv{sm.id, sm.term, false}})
			return responses
		}
		if sm.term < ev.term || (sm.term == ev.term && sm.votedFor == -1) { //TODO: Discuss the condition
			lastTermV := sm.log[len(sm.log) - 1].term

			if (lastTermV > ev.recentLogTerm) || (lastTermV == ev.recentLogTerm && len(sm.log) - 1 > ev.recentLogIndex) {
				append(responses, Send{ev.candidateId, VoteRespEv{sm.id, sm.term, false}})
				if sm.term < ev.term {
					sm.term = ev.term
					sm.votedFor = -1
					append(responses, StateStore{sm.term, sm.votedFor})
				}
			} else {
				sm.votedFor = ev.candidateId
				append(responses, Send{ev.candidateId, VoteRespEv{sm.id, ev.term, true}})
				append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
				if sm.term < ev.term {
					sm.term = ev.term
					append(responses, StateStore{sm.term, sm.votedFor})
				}
			}

		}
	case 2, 3:
		if sm.term < ev.term {
			sm.term = ev.term
			append(responses, Alarm{random(sm.timeout, sm.timeout * 2)})
			sm.voteCount = 0
			sm.votedFor = -1
			sm.state = 1
			append(responses, StateStore{sm.term, sm.votedFor})
			append(responses, sm.handleVoteReq(ev)...)
		} else {
			append(responses, Send{ev.candidateId, VoteRespEv{sm.id, sm.term, false}})
		}
	}
	return responses
}

func (sm *StateMachine) handleVoteResp(ev VoteRespEv) []interface{} {
	var responses []interface{}
	switch sm.state {
	case 1:
		if sm.term < ev.term {
			sm.term = ev.term
			sm.votedFor = -1
			append(responses, StateStore{sm.term, sm.votedFor})
		}
	case 2:
		if sm.term < ev.term {
			sm.term = ev.term
			sm.votedFor = -1
			sm.voteCount = 0
			append(responses, StateStore{sm.term, sm.votedFor})
			append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
			sm.state = 1
		} else if sm.term == ev.term {
			if ev.success {
				sm.voteCount++
				var majority int
				majority = (len(sm.peers)/2+1)
				if sm.voteCount >= majority {
					sm.state = 3
					leaderLastIndex := len(sm.log)
					for peer:= range sm.peers {
						sm.nextIndex[peer] = leaderLastIndex
						sm.matchIndex[peer] = 0
						append(responses,sm.sendHeartBeat()...)
					}
				}
			}

		} else {
			// do nothing
		}

	case 3:
		if sm.term < ev.term {
			sm.term = ev.term
			sm.votedFor = -1
			sm.voteCount = 0
			append(responses, StateStore{sm.term, sm.votedFor})
			append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
			sm.state = 1
		}
	}

	return responses
}



func (sm *StateMachine) ProcessEvent(ev interface{}) {
	switch ev.(type) {
	case AppendEv:
		cmd := ev.(AppendEv)
		sm.handleAppend(cmd)
	case TimeoutEv:
		cmd := ev.(TimeoutEv)
		sm.handleTimeout(cmd)
	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)
		sm.handleAppendEntriesReq(cmd)
	case AppendEntriesRespEv:
		cmd := ev.(AppendEntriesReqEv)
		sm.handleAppendEntriesResp(cmd)
	case VoteReqEv:
		cmd := ev.(VoteReqEv)
		sm.handleVoteReq(cmd)
	case VoteRespEv:
		cmd := ev.(VoteReqEv)
		sm.handleVoteResp(cmd)

	}
}

func main() {
	rand.Seed(99)
	var sm StateMachine

	sm.ProcessEvent(AppendEntriesReqEv{term: 10, prevLogIndex: 100, prevLogTerm: 3})
}
