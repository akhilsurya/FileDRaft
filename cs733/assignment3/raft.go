package main

import (
	"encoding/gob"
	"errors"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"io/ioutil"
	"math/rand"
	"os"
	logger "log"
	"strconv"
	"strings"
	"time"
)

type CommitInfo struct {
	Data  []byte
	Index int
	Err   error
}

type Config struct {
	cluster          []NetConfig
	Id               int
	LogDir           string
	ElectionTimeout  int
	HeartbeatTimeout int
}

type NetConfig struct {
	Id   int
	Host string
	Port int
}

type Node interface {
	Append(content []byte)
	CommitChannel() chan CommitInfo
	// Last known committed index in the log. This could be -1 until the system stabilizes.
	CommittedIndex() int
	// Returns the data at a log index, or an error.
	Get(index int) (error, []byte)
	// Node's id
	Id() int
	// Id of leader. -1 if unknown
	LeaderId() int
	// Signal to shut down all go routines, stop sockets, flush log and close it, cancel timers.
	Shutdown()
}

type RaftNode struct {
	Node
	sm            StateMachine
	server        cluster.Server
	lg            *log.Log
	commitChannel chan CommitInfo
	eventChannel  chan interface{}
	timer         *time.Timer // Send on this channel to reset timer
	shutDownChan  chan int    // Send something to shut the node down
}

func (rn *RaftNode) CommitChannel() chan CommitInfo {
	return rn.commitChannel
}

func (rn *RaftNode) Id() int {
	// Get from sm
	return rn.sm.id
}

func (rn *RaftNode) CommittedIndex() int {
	return rn.sm.commitIndex
}

func (rn *RaftNode) LeaderId() int {
	return rn.sm.leaderId
}

func (rn *RaftNode) Shutdown() {
	rn.lg.Close()
	rn.server.Close()
	rn.timer.Stop()
	rn.shutDownChan <- 1
	logger.Println("Shut down successful for : ", rn.Id())
}

func (rn *RaftNode) Get(i int) (error, []byte) {
	content, err := rn.lg.Get(int64(i))
	logEntry := content.(LogEntry)
	return err, logEntry.Command
}

func NetToPeersConfig(addresses []NetConfig) []cluster.PeerConfig {
	peerConfigs := make([]cluster.PeerConfig, len(addresses))
	for i, peerAddress := range addresses {
		address := peerAddress.Host + ":" + strconv.Itoa(peerAddress.Port)
		peerConfigs[i] = cluster.PeerConfig{peerAddress.Id, address}
	}
	return peerConfigs
}

func getPeers(peerConfigs []NetConfig) []int {
	peers := make([]int, len(peerConfigs))
	for i, peerConfig := range peerConfigs {
		peers[i] = peerConfig.Id
	}
	return peers
}

func (rn *RaftNode) Append(content []byte) {
	logger.Println("New request from client")
	ev := AppendEv{content}
	// Expect error in Commit response if this node isn't leader
	rn.eventChannel <- ev // Best part of Go :)
}

func readState(id int) (term int, votedFor int) {
	// Assumes already exists
	content, _ := ioutil.ReadFile(strconv.Itoa(id) + "_state")
	c := string(content)
	fields := strings.Fields(c)
	term, _ = strconv.Atoi(fields[0])
	votedFor, _ = strconv.Atoi(fields[1])
	return term, votedFor
}

func New(nodeConfig Config) (Node, error) {
	clusterConfig := cluster.Config{Peers: NetToPeersConfig(nodeConfig.cluster), InboxSize: 50, OutboxSize: 50}
	server, err := cluster.New(nodeConfig.Id, clusterConfig)
	if err != nil {
		return nil, errors.New("Could not start the messaging service")
	}

	lg, err := log.Open(nodeConfig.LogDir)
	if err != nil {
		return nil, errors.New("Could not start log service")
	}
	lg.RegisterSampleEntry(LogEntry{})

	commitChannel := make(chan CommitInfo)
	eventChannel := make(chan interface{})
	shutdownChan := make(chan int)
	initLog := make([]LogEntry, 0)
	initLog = append(initLog, LogEntry{0, make([]byte, 0)})
	votedFor := -1
	term := 0
	_, err = os.Stat(strconv.Itoa(nodeConfig.Id) + "_state")
	if err == nil {
		// State file already exists, so restart read vars from it
		term, votedFor = readState(nodeConfig.Id)
		// restore log entries from log saved on disk
		logLastIndex := lg.GetLastIndex()
		if logLastIndex != -1 {
			logger.Println(nodeConfig.Id, " : Last index on disk : ", logLastIndex)
			for i := 0; int64(i) < logLastIndex; i++ {
				entry, _ := lg.Get(int64(i))
				initLog = append(initLog, entry.(LogEntry))
			}
		}

	}

	sm := StateMachine{nodeConfig.Id, getPeers(nodeConfig.cluster), term,
		votedFor, 1, initLog, make(map[int]int), make(map[int]int),
		0, nodeConfig.ElectionTimeout, make(map[int]int), -1}

	rn := RaftNode{sm: sm, server: server, lg: lg, commitChannel: commitChannel, eventChannel: eventChannel, shutDownChan: shutdownChan}
	timerFunc := func(eventChannel chan interface{}) func() {
		return func() {
			rn.eventChannel <- TimeoutEv{}
		}
	}
	rn.timer = time.AfterFunc(time.Duration(random(sm.timeout, 2*sm.timeout)), timerFunc(rn.eventChannel))

	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesRespEv{})
	gob.Register(VoteReqEv{})
	gob.Register(VoteRespEv{})
	go rn.ProcessEvents()
	return &rn, nil
}

func (rn *RaftNode) saveState(state StateStore) error {
	content := strconv.Itoa(state.term) + " " + strconv.Itoa(state.votedFor)
	err := ioutil.WriteFile(strconv.Itoa(rn.Id())+"_state", []byte(content), 0644)
	return err
}

func (rn *RaftNode) resetTimer(timeout int) {
	rn.timer.Reset(time.Duration(timeout) * time.Millisecond)
	logger.Println("Resetting timer for : ", rn.sm.id)
	return
}

func (rn *RaftNode) handleSMResponses(resp []interface{}) {
	logger.Println("Node ", rn.Id(), " processing ", len(resp), " responses")
	for _, ev := range resp {
		logger.Println(rn.Id(), " : ", ev)
		switch ev.(type) {
		case Send:
			logger.Println("Send request from : ", rn.Id())
			sendRequest := ev.(Send)
			logger.Println("Sending message to ", sendRequest.peer)
			if !rn.server.IsClosed() {
				rn.server.Outbox() <- &cluster.Envelope{Pid: sendRequest.peer, Msg: sendRequest.event}
			} else {
				// TODO :
				return
			}

		case Commit:
			comm := ev.(Commit)
			var cm CommitInfo
			if comm.err != "" {
				// -1 to match the ignore the dummy
				cm = CommitInfo{Data: comm.data, Index: comm.index - 1, Err: errors.New(comm.err)}
			} else {
				cm = CommitInfo{Data: comm.data, Index: comm.index - 1, Err: nil}
			}

			rn.commitChannel <- cm
		case Alarm:
			logger.Println("Reset timer request from ", rn.Id())
			alarm := ev.(Alarm)
			rn.resetTimer(alarm.t)
			logger.Println("Reset done ")
		case LogStore:
			logRequest := ev.(LogStore)
			logger.Println("Trying to store ", logRequest.data)
			lastIndex := rn.lg.GetLastIndex()
			logger.Println(rn.Id(), " : Last Index before inserting is ", lastIndex)
			curIndex := logRequest.index - 1 // To account for dummy
			logger.Println("LAST INDEX +1 : ", lastIndex+1, " CURINDEX : ", curIndex)
			if lastIndex+1 == int64(curIndex) {
				logger.Println("Should be coming here to save")
				rn.lg.Append(LogEntry{logRequest.term, logRequest.data})
				logger.Println("Last Index after inserting is ", rn.lg.GetLastIndex())
			} else if lastIndex+1 > int64(curIndex) {
				rn.lg.TruncateToEnd(int64(curIndex))
				rn.lg.Append(LogEntry{logRequest.term, logRequest.data})
			} else {
				// TODO :
				for {
					lastIndex = rn.lg.GetLastIndex()
					if lastIndex+1 == int64(curIndex) {
						logger.Println("Broken out ")
						logger.Println("Should be coming here to save")
						rn.lg.Append(LogEntry{logRequest.term, logRequest.data})
						logger.Println("Last Index after inserting is ", rn.lg.GetLastIndex())
						continue
					} else {
						logger.Println(rn.Id(), " : Waiting for others : ", curIndex, " , ", lastIndex)
						time.Sleep(10 * time.Millisecond)
					}

				}

			}
		case StateStore:
			state := ev.(StateStore)
			err := rn.saveState(state)
			if err != nil {
				logger.Println("Unexpected error while saving")
				// TODO : shut down ?
			}
			logger.Println("State store done for", rn.Id())
		default:
			logger.Println("Unknown response from state machine")
		}
	}
}

func (rn *RaftNode) ProcessEvents() {
	for {
		select {
		case ev, ok := <-rn.eventChannel:
			if ok {
				logger.Println("New event in chanel of ", rn.sm.id)
				resp := rn.sm.ProcessEvent(ev)
				logger.Println("Got them responses says : ", rn.sm.id)
				go rn.handleSMResponses(resp)
			} else {
				logger.Println("Stopped processing on ", rn.Id())
				return
			}

		case msg, ok := <-rn.server.Inbox():
			// check message id ?
			if ok {
				ev := msg.Msg
				resp := rn.sm.ProcessEvent(ev)
				rn.handleSMResponses(resp)
			} else {
				logger.Println("Stopped processing on ", rn.Id())
				return
			}

		case <-rn.shutDownChan:
			return
		}
	}

}

func main() {
	rand.Seed(4)

}
