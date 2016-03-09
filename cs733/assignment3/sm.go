package main

import (
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
	matchIndex  map[int]int
	commitIndex int
	timeout     int
	votes       map[int]int // 0 not vote, 1 voted, -1 denied
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (sm *StateMachine) resetVotes() {
	sm.votes = make(map[int]int)
	for _, peer := range sm.peers {
		sm.votes[peer] = 0
	}

}

func (sm *StateMachine) hasWon() bool {
	votesGranted := 0
	for _, peer := range sm.peers {
		if sm.votes[peer] == 1 {
			votesGranted++
		}
	}
	var majority int
	majority = (len(sm.peers)/2 + 1)
	return votesGranted >= majority
}

func (sm *StateMachine) hasLost() bool {
	votesDenied := 0
	for _, peer := range sm.peers {
		if sm.votes[peer] == -1 {
			votesDenied++
		}
	}
	var majority int
	majority = (len(sm.peers)/2 + 1)
	return votesDenied >= majority
}

func (sm *StateMachine) propagate() []interface{} {
	responses := make([]interface{}, 0)
	for peer := range sm.peers {
		if peer != sm.id {
			prevIndex := sm.nextIndex[peer] - 1
			event := AppendEntriesReqEv{sm.id, sm.term, prevIndex, sm.log[prevIndex].term, sm.log[prevIndex+1 : len(sm.log)], sm.commitIndex}
			responses = append(responses, Send{peer, event})
		}
	}
	return responses
}

func (sm *StateMachine) requestVote() []interface{} {
	responses := make([]interface{}, 0)
	for peer := range sm.peers {
		if peer != sm.id {
			event := VoteReqEv{sm.id, sm.term, len(sm.log) - 1, sm.log[len(sm.log)-1].term}
			responses = append(responses, Send{peer, event})
		}
	}
	return responses
}

func (sm *StateMachine) sendHeartBeat() []interface{} {
	responses := make([]interface{}, 0)
	for peer := range sm.peers {
		if peer != sm.id {
			prevIndex := sm.nextIndex[peer] - 1
			event := AppendEntriesReqEv{sm.id, sm.term, prevIndex, sm.log[prevIndex].term, make([]LogEntry, 0), sm.commitIndex}
			responses = append(responses, Send{peer, event})
		}
	}
	// reset timer
	responses = append(responses, Alarm{sm.timeout})
	return responses
}

func (sm *StateMachine) checkAndUpdateLog(prevLogIndex, prevLogTerm, commitIndex, leader int, entries []LogEntry) []interface{} {
	responses := make([]interface{}, 0)
	if prevLogIndex > len(sm.log)-1 || sm.log[prevLogIndex].term != prevLogTerm {
		responses = append(responses, Send{leader, AppendEntriesRespEv{sm.id, sm.term, false, 0}}) // matching should not matter
	} else {
		// Handle out of order
		if len(sm.log) >= prevLogIndex+len(entries) && sm.log[len(sm.log)-1].term == sm.term {
			// Must be out of order
			responses = append(responses, Send{leader, AppendEntriesRespEv{sm.id, sm.term, true, len(sm.log) - 1}})
			return responses
		}
		sm.log = sm.log[0 : prevLogIndex+1]
		sm.log = append(sm.log, entries...)
		for i := range entries {
			responses = append(responses, LogStore{prevLogIndex + 1 + i, entries[i].term, entries[i].command})
		}
		if sm.commitIndex < commitIndex {
			sm.commitIndex = min(commitIndex, len(sm.log)-1)
		}

		responses = append(responses, Send{leader, AppendEntriesRespEv{sm.id, sm.term, true, prevLogIndex + len(entries)}})
	}
	return responses
}

func (sm *StateMachine) checkForCommit() []interface{} {
	responses := make([]interface{}, 0)
	oldCommitIndex := sm.commitIndex
	curIndex := len(sm.log) - 1

	for {
		count := 0
		var majority int
		majority = len(sm.peers) / 2 // Excluding self
		//fmt.Println("Majority is ", majority)
		for peer := range sm.peers {
			if sm.matchIndex[peer] >= curIndex {
				count++
			}
		}
		if count >= majority && sm.log[curIndex].term == sm.term {
			sm.commitIndex = curIndex
			for i := oldCommitIndex + 1; i <= curIndex; i++ {
				responses = append(responses, Commit{i, sm.log[i].command, ""})
			}
			break
		} else {
			if curIndex == oldCommitIndex {
				return responses
			} else {
				curIndex--
			}
		}
	}
	return responses
}

func (sm *StateMachine) handleAppend(ev AppendEv) []interface{} {
	responses := make([]interface{}, 0)
	switch sm.state {
	case 1, 2:
		responses = append(responses, Commit{0, ev.data, "INCORRECT_HOST"})
	case 3:
		newEntry := LogEntry{sm.term, ev.data}
		responses = append(responses, LogStore{len(sm.log), sm.term, ev.data})
		sm.log = append(sm.log, newEntry)
		responses = append(responses, sm.propagate()...)
	}
	return responses
}

func (sm *StateMachine) handleTimeout(ev TimeoutEv) []interface{} {
	responses := make([]interface{}, 0)
	switch sm.state {
	case 1, 2:
		responses = append(responses, StateStore{sm.term + 1, sm.id})
		sm.term++
		sm.votedFor = sm.id
		sm.resetVotes()
		sm.votes[sm.id] = 1
		sm.state = 2
		responses = append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
		responses = append(responses, sm.requestVote()...)
	case 3:
		responses = append(responses, Alarm{sm.timeout})
		responses = append(responses, sm.sendHeartBeat()...)
	}
	return responses
}

func (sm *StateMachine) handleAppendEntriesReq(ev AppendEntriesReqEv) []interface{} {
	responses := make([]interface{}, 0)
	switch sm.state {
	case 1:
		if ev.term < sm.term {
			responses = append(responses, Send{ev.leader, AppendEntriesRespEv{sm.id, sm.term, false, 0}})
			return responses
		} else if ev.term > sm.term {
			sm.term = ev.term
			sm.votedFor = -1
			sm.resetVotes()
			responses = append(responses, StateStore{ev.term, -1})
		}
		responses = append(responses, Alarm{random(sm.timeout, 2*sm.timeout)}) // TODO : Correct timing ?
		responses = append(responses, sm.checkAndUpdateLog(ev.prevLogIndex, ev.prevLogTerm, ev.commitIndex, ev.leader, ev.entries)...)
	case 2, 3:
		// Equality not possible in case of leader
		if ev.term > sm.term {
			sm.resetVotes()
			sm.term = ev.term
			sm.votedFor = -1
			sm.state = 1
			responses = append(responses, StateStore{ev.term, -1})
			responses = append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
			responses = append(responses, sm.checkAndUpdateLog(ev.prevLogIndex, ev.prevLogTerm, ev.commitIndex, ev.leader, ev.entries)...)
		} else {
			responses = append(responses, Send{ev.leader, AppendEntriesRespEv{sm.id, sm.term, false, 0}})
		}
	}
	return responses
}

func (sm *StateMachine) handleAppendEntriesResp(ev AppendEntriesRespEv) []interface{} {
	responses := make([]interface{}, 0)
	switch sm.state {
	case 1:
		if ev.term > sm.term {
			sm.term = ev.term
			sm.votedFor = -1
			responses = append(responses, StateStore{sm.term, -1})
		}
	case 2:
		if ev.term > sm.term {
			sm.term = ev.term
			sm.resetVotes()
			sm.votedFor = -1
			responses = append(responses, StateStore{sm.term, -1})
		}
	case 3:
		if ev.term < sm.term {
			// empty
			return responses
		}
		if ev.term > sm.term {
			sm.term = ev.term
			sm.votedFor = -1
			responses = append(responses, StateStore{sm.term, -1})
			sm.state = 1
			sm.resetVotes()
			sm.votes[sm.id] = 1
			return responses
		}

		if ev.success {
			// Should handle out of order delivery
			sm.matchIndex[ev.peerId] = max(ev.matchedTill, sm.matchIndex[ev.peerId])
			sm.nextIndex[ev.peerId] = len(sm.log)
			responses = append(responses, sm.checkForCommit()...)
		} else {
			var index int
			// TODO : Handle out of order delivery
			//if ev.< sm.matchIndex[] {
			//
			//} else {
			if sm.nextIndex[ev.peerId] == 1 {
				index = 1
			} else {
				sm.nextIndex[ev.peerId]--
				index = sm.nextIndex[ev.peerId]
			}
			responses = append(responses, Send{ev.peerId, AppendEntriesReqEv{sm.id, sm.term, index - 1, sm.log[index-1].term, sm.log[index:len(sm.log)], sm.commitIndex}})

			//}
		}
	}
	return responses
}

func (sm *StateMachine) handleVoteReq(ev VoteReqEv) []interface{} {
	responses := make([]interface{}, 0)
	switch sm.state {
	case 1:
		if sm.term > ev.term {
			responses = append(responses, Send{ev.candidateId, VoteRespEv{sm.id, sm.term, false}})
			return responses
		}
		if sm.term < ev.term || (sm.term == ev.term && sm.votedFor == -1) {
			lastTermV := sm.log[len(sm.log)-1].term

			if (lastTermV > ev.recentLogTerm) || (lastTermV == ev.recentLogTerm && len(sm.log)-1 > ev.recentLogIndex) {
				responses = append(responses, Send{ev.candidateId, VoteRespEv{sm.id, sm.term, false}})
				if sm.term < ev.term {
					sm.term = ev.term
					sm.votedFor = -1
					responses = append(responses, StateStore{sm.term, sm.votedFor})
				}
			} else {
				sm.votedFor = ev.candidateId
				responses = append(responses, Send{ev.candidateId, VoteRespEv{sm.id, ev.term, true}})
				responses = append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
				if sm.term < ev.term {
					sm.term = ev.term
					responses = append(responses, StateStore{sm.term, sm.votedFor})
				}
			}

		}
	case 2, 3:
		if sm.term < ev.term {
			sm.term = ev.term
			responses = append(responses, Alarm{random(sm.timeout, sm.timeout*2)})
			sm.resetVotes()
			sm.votedFor = -1
			sm.state = 1
			responses = append(responses, StateStore{sm.term, sm.votedFor})
			responses = append(responses, sm.handleVoteReq(ev)...)
		} else {
			responses = append(responses, Send{ev.candidateId, VoteRespEv{sm.id, sm.term, false}})
		}
	}
	return responses
}

func (sm *StateMachine) handleVoteResp(ev VoteRespEv) []interface{} {
	responses := make([]interface{}, 0)
	switch sm.state {
	case 1:
		if sm.term < ev.term {
			sm.term = ev.term
			sm.votedFor = -1
			responses = append(responses, StateStore{sm.term, sm.votedFor})
		}
	case 2:
		if sm.term < ev.term {
			sm.term = ev.term
			sm.votedFor = -1
			sm.resetVotes()
			responses = append(responses, StateStore{sm.term, sm.votedFor})
			responses = append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
			sm.state = 1
		} else if sm.term == ev.term {
			if ev.success {
				sm.votes[ev.peerId] = 1

				if sm.hasWon() {
					sm.state = 3
					leaderLastIndex := len(sm.log)
					for peer := range sm.peers {
						sm.nextIndex[peer] = leaderLastIndex
						sm.matchIndex[peer] = 0
					}
					responses = append(responses, sm.sendHeartBeat()...)
				}
			} else {
				sm.votes[ev.peerId] = -1

				if sm.hasLost() {
					sm.state = 1
					sm.resetVotes()
					// term and votedFor remain same
				}
			}

		} else {
			// do nothing
		}

	case 3:
		if sm.term < ev.term {
			sm.term = ev.term
			sm.votedFor = -1
			sm.resetVotes()
			responses = append(responses, StateStore{sm.term, sm.votedFor})
			responses = append(responses, Alarm{random(sm.timeout, 2*sm.timeout)})
			sm.state = 1
		}
	}

	return responses
}

func (sm *StateMachine) ProcessEvent(ev interface{}) []interface{} {
	responses := make([]interface{}, 0)
	switch ev.(type) {
	case AppendEv:
		cmd := ev.(AppendEv)
		responses = sm.handleAppend(cmd)
	case TimeoutEv:
		cmd := ev.(TimeoutEv)
		responses = sm.handleTimeout(cmd)
	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)
		responses = sm.handleAppendEntriesReq(cmd)
	case AppendEntriesRespEv:
		cmd := ev.(AppendEntriesRespEv)
		responses = sm.handleAppendEntriesResp(cmd)
	case VoteReqEv:
		cmd := ev.(VoteReqEv)
		responses = sm.handleVoteReq(cmd)
	case VoteRespEv:
		cmd := ev.(VoteRespEv)
		responses = sm.handleVoteResp(cmd)
	}
	return responses
}

func main() {
	rand.Seed(99)
	var sm StateMachine

	sm.ProcessEvent(AppendEntriesReqEv{term: 10, prevLogIndex: 100, prevLogTerm: 3})
}
