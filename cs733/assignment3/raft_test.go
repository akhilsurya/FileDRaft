package main

import (
	"fmt"
	"strconv"
	"testing"
	"time"
	"math/rand"
	"bytes"
)

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
		config := Config{cluster, 100 * i, "logs/"+strconv.Itoa(i * 100), i*1000, i*100}
		tmp, err := New(config)
		if err != nil {
			fmt.Sprintf("Error while starting node with id; %d", 100*i, err)
		}
		rafts[i-1] = tmp
	}
	return rafts
}

func TestBasic(t *testing.T) {
	rafts := initRaft()
	time.Sleep(7*time.Second)
	fmt.Println("Done sleeping1")
	leader := -1
	for i :=0; i< 5; i++ {
		if rafts[i].LeaderId() != -1 {
			leader = rafts[i].LeaderId()
			fmt.Printf("Found leader : %d", (i+1)*100)
		}
	}
	fmt.Println("Leader is ", leader)
	rafts[0].Append([]byte("Testing"))
	resp := <-rafts[0].CommitChannel()
	expect(t, resp.Index, 1)
	matchBytes(t, []byte("Testing"), resp.Data)
	fmt.Println("first part of test done")
	rafts[0].Shutdown()
	time.Sleep(7*time.Second)
	fmt.Println("Done sleeping2")
	leader = -1
	// Since 0 is shutdown
	for i := 1; i<5; i++ {
		if rafts[i].LeaderId() != -1  {
			leader = rafts[i].LeaderId()
			fmt.Printf("Found leader2 : %d\n", (i+1)*100)
		}
	}

	for i := 1; i < 5; i++ {
		rafts[i].Shutdown()
	}
}

func matchBytes(t *testing.T, a []byte, b []byte) {
	if !bytes.Equal(a, b) {
		t.Error(fmt.Sprintf("Expected %#v, found %#v", a, b))
	}
}