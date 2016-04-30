package main

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
	"fmt"
)

func cleanUp() {
	removeContents("logs")
	for i := 1; i < 6; i++ {
		os.Remove(strconv.Itoa(i*100) + "_state")
	}
}

func initRaft() []Node {
	rand.Seed(4)
	rafts := make([]Node, 5)
	cluster := []NetConfig{
		NetConfig{100, "localhost", 8080},
		NetConfig{200, "localhost", 8081},
		NetConfig{300, "localhost", 8082},
		NetConfig{400, "localhost", 8083},
		NetConfig{500, "localhost", 8084},
	}
	for i := 1; i < 6; i++ {
		config := Config{cluster, 100 * i, "logs/" + strconv.Itoa(i*100), i * 1000, i * 100}
		tmp, err := New(config)
		if err != nil {
			fmt.Sprintf("Error while starting node with id; %d", 100*i, err)
		}
		rafts[i-1] = tmp
	}
	return rafts
}

// Ref : http://stackoverflow.com/questions/33450980/golang-remove-all-contents-of-a-directory
func removeContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

// Send a message to the leader and check for commit
// Also leader going down and someone else getting elected
func TestRaft1(t *testing.T) {
	cleanUp()
	rafts := initRaft()
	time.Sleep(7 * time.Second)
	leader := -1
	leaderIndex := -1
	for i := 0; i < 5; i++ {
		if rafts[i].LeaderId() != -1 && leader == -1 {
			leader = rafts[i].LeaderId()
			leaderIndex = i
			//fmt.Printf("Found leader : %d\n", (i+1)*100)
		}
	}
	//fmt.Println("Leader is ", leader)
	rafts[leaderIndex].Append([]byte("Testing"))
	resp := <-rafts[leaderIndex].CommitChannel()
	expect(t, resp.Index, 0)
	err, found := rafts[leaderIndex].Get(0)
	expectTruth(t, err == nil)
	matchBytes(t, found, resp.Data)
	time.Sleep(time.Second)
	// Should have propagated
	otherNodeIndex := 4
	err, found = rafts[otherNodeIndex].Get(0)
	matchBytes(t, found, resp.Data)
	rafts[leaderIndex].Shutdown()
	time.Sleep(time.Second)

	leader = -1
	for i := 1; i < 5; i++ {
		if rafts[i].LeaderId() != -1 {
			leader = rafts[i].LeaderId()
			//fmt.Printf("Found leader2 : %d\n", (i+1)*100)
		}
	}

	for i := 1; i < 5; i++ {
		rafts[i].Shutdown()
	}
}

// Shutdown everyone and get back up

func TestRaft3(t *testing.T) {
	cleanUp()
	rafts := initRaft()
	time.Sleep(5 * time.Second)
	leader := -1
	leaderIndex := -1
	for i := 0; i < 5; i++ {
		if rafts[i].LeaderId() != -1 && leader == -1 {
			leader = rafts[i].LeaderId()
			leaderIndex = i
			//fmt.Printf("Found leader : %d\n", (i+1)*100)
		}
	}
	//fmt.Println("Leader is ", leader)
	rafts[leaderIndex].Append([]byte("Testing"))
	resp := <-rafts[leaderIndex].CommitChannel()

	expect(t, resp.Index, 0)
	err, found := rafts[leaderIndex].Get(0)
	matchBytes(t, found, resp.Data)
	expectTruth(t, err == nil)

	// Shutting down 400, 500 to see where they pick up
	for i := 3; i < 5; i++ {
		rafts[i].Shutdown()
	}

	// Restarting all
	leader = -1
	leaderIndex = -1
	for i := 0; i < 5; i++ {
		if rafts[i].LeaderId() != -1 && leader == -1 {
			leader = rafts[i].LeaderId()
			leaderIndex = i
			//fmt.Printf("Found leader : %d\n", (i+1)*100)
		}
	}
	//fmt.Println("Leader after few shutdowns is : ", leader)
	rafts[leaderIndex].Append([]byte("Testing2"))
	resp = <-rafts[leaderIndex].CommitChannel()
	// Index should continue
	//fmt.Println("Receied commit response")
	expect(t, resp.Index, 1)
	err, found = rafts[leaderIndex].Get(1)
	matchBytes(t, found, resp.Data)
	expectTruth(t, err == nil)
	// Shutting down 100, 200, 300
	for i := 0; i < 3; i++ {
		rafts[i].Shutdown()
	}
}

func matchBytes(t *testing.T, a []byte, b []byte) {
	if !bytes.Equal(a, b) {
		t.Error(fmt.Sprintf("Expected %#v, found %#v", a, b))
	}
}

func assertNotEqual(t *testing.T, a int, b int) {
	if a == b {
		t.Error(fmt.Sprintf("Expected %#v and  found %#v same integer", a, b))
	}
}

func expectTruth(t *testing.T, b bool) {
	if !b {
		t.Error(fmt.Sprintf("Expected condition not satisfied"))
	}
}
