package main

import (
	"strconv"
	"time"
	"math/rand"
	"fmt"
	"os"
	"path/filepath"
)

var servers map[int]*FileServer
func start()  {
	servers = make(map[int]*FileServer)

	// Ports to communicate between RAFT nodes
	cluster := []NetConfig{
		NetConfig{100, "localhost", 8090},
		NetConfig{200, "localhost", 8091},
		NetConfig{300, "localhost", 8092},
		NetConfig{400, "localhost", 8093},
		NetConfig{500, "localhost", 8094},
	}

	// addresses for clients to communicate
	srvrAddrs := []NetConfig{
		NetConfig{100, "localhost", 8095},
		NetConfig{200, "localhost", 8096},
		NetConfig{300, "localhost", 8097},
		NetConfig{400, "localhost", 8098},
		NetConfig{500, "localhost", 8099},
	}

	for i := 1; i < 6; i++ {
		id := 100 * i
		nodeConfig := Config{cluster, id, "logs/" + strconv.Itoa(i*100), i * i * 10000, i * 100}
		servers[id] = serverMain(ServerConfig{nodeConfig, srvrAddrs, srvrAddrs[i-1].Host, srvrAddrs[i-1].Port, id})
	}
	// Waiting for leader
	time.Sleep(1 * time.Second)
}

func done() {
	for i := range servers {
		servers[i].shutdown()
	}
}

func cleanUp() {
	removeContents("logs")
	for i := 1; i < 6; i++ {
		os.Remove(strconv.Itoa(i*100) + "_state")
	}
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

func main() {
	cleanUp()
	rand.Seed(123)
	fmt.Println("Starting all the servers...")
	start()
	fmt.Println("Up and running...")
	defer done()
	for {

	}
}


func concat(a int, b int ) string {
	return strconv.Itoa(a)+"_"+strconv.Itoa(b)
}