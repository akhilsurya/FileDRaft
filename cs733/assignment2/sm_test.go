package main

import (
	"bytes"
	"fmt"
	"testing"
)

//- Follower responding to append entries
//	- from leader
//	- from previous term
//	- from future term
//
//- testing voting
//	- recentness check
//	- check satisified look for success
//	- not repeated voting
//	- sending request to candidate in same term
//	- sending request to stale leader
//	-
//- leader selection
//	- majority votes
//	- majority no votes
//
//- propogation
//	- leader ahead
//	- follower ahead
//	- both equal
//	- term issues
//
//-commit
//	- new leader not commite
//	- majority commit
//	- otherside handling commit index

func initSM(id int) StateMachine {
	initLog := make([]LogEntry, 0)
	initLog = append(initLog, LogEntry{0, make([]byte, 0)})
	var peers []int
	for i := 0; i < 5; i++ {
		peers = append(peers, i)
	}
	return StateMachine{
		id, peers, 0, -1, 1, initLog, make(map[int]int), make(map[int]int), 0, 200, make(map[int]int),
	}
}

/**

 */
func Test1(t *testing.T) {
	sm := initSM(0)
	var expected []interface{}
	// Timeout event
	responses := sm.ProcessEvent(TimeoutEv{})
	expected = append(expected, StateStore{1, 0})
	expected = append(expected, Alarm{230}) // time does not matter
	for i := 1; i < 5; i++ {
		expected = append(expected, Send{i, VoteReqEv{sm.id, 1, 0, 0}})
	}
	matchResponse(t, expected, responses)
	// Should be candidate
	expect(t, sm.state, 2)
	expect(t, sm.term, 1)
	expect(t, sm.votedFor, sm.id)
	expect(t, sm.votes[sm.id], 1)
	// Receive votes now
	responses = sm.ProcessEvent(VoteRespEv{1, 1, true})
	expect(t, sm.votes[1], 1)
	expect(t, sm.state, 2) // Should still be candidate
	// No in a vote
	responses = sm.ProcessEvent(VoteRespEv{2, 1, false})
	expect(t, sm.votes[2], -1)
	expect(t, sm.state, 2)
	expected = make([]interface{}, 0)
	matchResponse(t, expected, responses)
	// Send an Append, should throw an error
	content := make([]byte, 4)
	responses = sm.ProcessEvent(AppendEv{content})
	expected = append(expected, Commit{0, content, "INCORRECT_HOST"})
	matchResponse(t, expected, responses)
	// Got majority after this
	responses = sm.ProcessEvent(VoteRespEv{3, 1, true})
	expect(t, sm.votes[3], 1)
	expect(t, sm.state, 3)
	// Check heartbeats etc. are sent
	expected = make([]interface{}, 0)
	// TODO : Order shold not matter
	for i := range sm.peers {
		if i != sm.id {
			expected = append(expected, Send{i, AppendEntriesReqEv{sm.id, sm.term, 0, 0, make([]LogEntry, 0), 0}})
		}
	}
	expected = append(expected, Alarm{200})
	matchResponse(t, expected, responses)
	// Should gnore vote from 4
	responses = sm.ProcessEvent(VoteRespEv{4, 1, true})
	expected = make([]interface{}, 0)
	matchResponse(t, expected, responses)
	// Now and append should be handled before getting responses for heart beats
	responses = sm.ProcessEvent(AppendEv{content})
	expected = make([]interface{}, 0)
	expected = append(expected, LogStore{1, 1, content})
	logEntry := LogEntry{1, content}
	logsToAdd := make([]LogEntry, 0)
	logsToAdd = append(logsToAdd, logEntry)
	for i := range sm.peers {
		if i != sm.id {
			expected = append(expected, Send{i, AppendEntriesReqEv{0, 1, 0, 0, logsToAdd, 0}})
		}
	}
	matchResponse(t, expected, responses)
	expect(t, len(sm.log), 2)
	// Gain positive response one, should not be commited
	responses = sm.ProcessEvent(AppendEntriesRespEv{2, 1, true, 1})
	expected = make([]interface{}, 0)
	expect(t, sm.matchIndex[2], 1) // Append worked
	matchResponse(t, expected, responses)
	expect(t, sm.commitIndex, 0)
	//Send the heart beats responses in now i.e. out of order for 2
	for i := 1; i < 5; i++ {
		responses = sm.ProcessEvent(AppendEntriesRespEv{i, 1, true, 0})
	}
	// No commit response
	matchResponse(t, expected, responses)
	expect(t, 1, sm.matchIndex[2]) // should not have reverted back

	// Return majority responses by getting a positive reply from 1
	responses = sm.ProcessEvent(AppendEntriesRespEv{1, 1, true, 1})
	expected = append(expected, Commit{1, content, ""})
	matchResponse(t, expected, responses)
	expect(t, sm.commitIndex, 1)
	expect(t, sm.nextIndex[1], 2)
	// One response returned
	responses = sm.ProcessEvent(AppendEntriesRespEv{3, 1, true, 1})
	expected = make([]interface{}, 0)
	matchResponse(t, expected, responses)

}

func Test2(t *testing.T) {
	sm := initSM(2)
	responses := sm.ProcessEvent(TimeoutEv{})
	expected := make([]interface{}, 0)
	expected = append(expected, StateStore{1, 2})
	expected = append(expected, Alarm{230}) // time wouldn't be matched
	for i := 0; i < 5; i++ {
		if i != 2 {
			expected = append(expected, Send{i, VoteReqEv{sm.id, 1, 0, 0}})
		}
	}
	expect(t, sm.state, 2)
	expect(t, sm.term, 1)
	expect(t, sm.votedFor, 2)
	matchResponse(t, expected, responses)
	// negative votes from 0
	responses = sm.ProcessEvent(VoteRespEv{0, 1, false})
	expected = make([]interface{}, 0)
	matchResponse(t, expected, responses)
	expect(t, sm.votes[0], -1)
	// Duplicate votes, should not have lost after after this
	sm.ProcessEvent(VoteRespEv{1, 1, false})
	sm.ProcessEvent(VoteRespEv{1, 1, false})
	expect(t, sm.state, 2)
	expect(t, sm.votes[1], -1)
	// Should have moved to follower after this
	sm.ProcessEvent(VoteRespEv{3, 1, false})
	expect(t, sm.state, 1)
	expect(t, sm.term, 1) // term should not have changed
	// AppendEntries from leader
	responses = sm.ProcessEvent(AppendEntriesReqEv{0, 1, 0, 0, make([]LogEntry, 0), 0})
	expected = make([]interface{}, 0)
	expected = append(expected, Alarm{200})
	expected = append(expected, Send{0, AppendEntriesRespEv{2, 1, true, 0}})
	matchResponse(t, expected, responses)
	// Appending log entries from user, should get comitted
	content := make([]byte, 4)
	logEntry := LogEntry{1, content}
	logsToAdd := make([]LogEntry, 0)
	logsToAdd = append(logsToAdd, logEntry)
	responses = sm.ProcessEvent(AppendEntriesReqEv{0, 1, 0, 0, logsToAdd, 1})
	expected = make([]interface{}, 0)
	expected = append(expected, Alarm{200})
	expected = append(expected, LogStore{1, 1, content})
	expect(t, sm.commitIndex, 1)
	expect(t, len(sm.log), 2)
	expected = append(expected, Send{0, AppendEntriesRespEv{2, 1, true, 1}})
	matchResponse(t, expected, responses)
	// Out of order heart beat
	responses = sm.ProcessEvent(AppendEntriesReqEv{0, 1, 0, 0, make([]LogEntry, 0), 1})
	expected = make([]interface{}, 0)
	expected = append(expected, Alarm{200})
	expected = append(expected, Send{0, AppendEntriesRespEv{2,1,true, 1}})
	matchResponse(t, expected, responses)
	// timeout and start competing
	responses = sm.ProcessEvent(TimeoutEv{})
	expected = make([]interface{}, 0)
	expected = append(expected, StateStore{2,2})
	expected = append(expected, Alarm{200})
	for _, peer := range sm.peers {
		if peer != 2 {
			expected = append(expected, Send{peer, VoteReqEv{2, 2, 1, 1}})
		}
	}
	expect(t, sm.state, 2)
	expect(t, sm.votedFor, 2)
	matchResponse(t, expected, responses)
	// Old leader's heart beat reaches
	responses = sm.ProcessEvent(AppendEntriesReqEv{0, 1, 1, 1, make([]LogEntry, 0), 1})
	expected = make([]interface{}, 0)
	expected = append(expected, Send{0, AppendEntriesRespEv{2, 2, false, 0}})
	matchResponse(t, expected, responses)
	expect(t, sm.state,2)

}
/**
Testing leader only commiting on getting a log entry from current term on majority
 */
func Test3(t *testing.T) {
	sm := initSM(0)
	sm.state = 3
	sm.commitIndex = 1
	sm.matchIndex[1] = 2
	sm.matchIndex[2] = 2
	sm.term = 3
	logOf0 := make([]LogEntry, 0)
	logOf0 = append(logOf0, LogEntry{0, make([]byte, 0)})
	logOf0 = append(logOf0, LogEntry{1, make([]byte, 0)})
	logOf0 = append(logOf0, LogEntry{2, make([]byte, 4)})
	sm.log = logOf0
	responses := sm.ProcessEvent(AppendEntriesRespEv{2, 3, true, 2})
	expected := make([]interface{}, 0)
	matchResponse(t, expected, responses)
}

func expect(t *testing.T, a int, b int) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}

func matchResponse(t *testing.T, expected []interface{}, responses []interface{}) {
	if len(expected) != len(responses) {
		t.Error(fmt.Sprintf("Number of responses not matching %#v, found %#v", expected, responses))
		return
	}
	for i := 0; i < len(expected); i++ {
		switch expected[i].(type) {
		case Alarm:
			switch responses[i].(type) {
			case Alarm:
			default:
				t.Error(fmt.Println("Expected type and found type did not match"))
			}

		case AppendEntriesReqEv:
			switch responses[i].(type) {
			case AppendEntriesReqEv:
				e := expected[i].(AppendEntriesReqEv)
				r := responses[i].(AppendEntriesReqEv)
				if !compareAppendEntriesReqEv(e, r) {
					t.Error(fmt.Sprintf("Expected %#v, found %#v", expected[i], responses[i]))
				}

			default:
				t.Error(fmt.Println("Expected type and found type did not match"))
			}
		case Send:
			switch responses[i].(type) {
			case Send:
				e := expected[i].(Send)
				r := responses[i].(Send)
				if !compareSend(e, r) {
					t.Error(fmt.Sprintf("Expected %#v, found %#v", expected[i], responses[i]))
				}
			default:
				t.Error(fmt.Sprintf("Expected %#v, found %#v", expected[i], responses[i]))
			}
		case Commit:
			switch responses[i].(type) {
			case Commit:
				e := expected[i].(Commit)
				r := responses[i].(Commit)
				if e.index != r.index || e.index != r.index || !bytes.Equal(e.data, r.data) {
					t.Error(fmt.Sprintf("Expected %#v, found %#v", expected[i], responses[i]))
				}
			default:
				t.Error(fmt.Sprintf("Expected %#v, found %#v", expected[i], responses[i]))
			}
		case LogStore:
			switch responses[i].(type) {
			case LogStore:
				e := expected[i].(LogStore)
				r := responses[i].(LogStore)
				if e.index != r.index || e.term != r.term || !bytes.Equal(e.data, r.data) {
					t.Error(fmt.Sprintf("Expected %#v, found %#v", expected[i], responses[i]))
				}
			default:
				t.Error(fmt.Sprintf("Expected %#v, found %#v", expected[i], responses[i]))
			}
		default:
			if expected[i] != responses[i] {
				t.Error(fmt.Sprintf("Expected %#v, found %#v", expected[i], responses[i]))
			}
		}
	}

}

func compareLogs(a, b []LogEntry) bool {
	if len(a) == len(b) {
		for i := range a {
			if a[i].term != b[i].term || !bytes.Equal(a[i].command, b[i].command) {
				return false
			}
		}
		return true
	} else {

		return false
	}
}

func compareAppendEntriesReqEv(e, r AppendEntriesReqEv) bool {
	if e.leader != r.leader || e.term != r.term || e.commitIndex != r.commitIndex || e.prevLogIndex != r.prevLogIndex || e.prevLogTerm != r.prevLogTerm || !compareLogs(e.entries, r.entries) {
		return false
	}
	return true
}

func compareSend(e, r Send) bool {
	if e.peer != r.peer {
		return false
	} else {
		switch e.event.(type) {
		case AppendEntriesReqEv:
			x := e.event.(AppendEntriesReqEv)
			switch r.event.(type) {
			case AppendEntriesReqEv:
				return compareAppendEntriesReqEv(x, r.event.(AppendEntriesReqEv))
			default:

				return false
			}
		default:
			return e.event == r.event
		}
	}

}
