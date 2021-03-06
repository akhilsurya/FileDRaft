package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var srvrs map[int]*FileServer
var srvrAddrs []NetConfig

func shutdownAll() {
	for i := range servers {
		servers[i].shutdown()
	}
}

func initSystem() {
	srvrs = make(map[int]*FileServer)
	// Remove saved logs for new test
	cleanUp()
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
		srvrs[id] = serverMain(ServerConfig{nodeConfig, srvrAddrs, srvrAddrs[i-1].Host, srvrAddrs[i-1].Port, id})
	}
	// Waiting for leader
	time.Sleep(1 * time.Second)
}
// Assumes 400 is up
func findLeader(t *testing.T) string {
	for {
		//fmt.Println(srvrs[400].addr+":"+strconv.Itoa(srvrs[400].port))
		cl := mkClient(t, srvrs[400].addr+":"+strconv.Itoa(srvrs	[400].port))
		defer cl.close()
		m, _ := cl.read("NO_FILE")
		if m.Kind == 'L' {
			//fmt.Println("Got a reply of type L with URL : ", string(m.Contents))
			url := string(m.Contents)
			// Redirect URL
			if url != "_" {
				//fmt.Println("Settled for URL : ", url)
				return url
			}
		} else if m.Kind == 'F' {
			// This is the leader
			return srvrs[400].addr + ":" + strconv.Itoa(servers[400].port)
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

func expectMessage(t *testing.T, response *Msg, expected *Msg, errstr string, err error) {
	if err != nil {
		t.Fatal("Unexpected error: " + err.Error())
	}
	ok := true
	if response.Kind != expected.Kind {
		ok = false
		errstr += fmt.Sprintf(" Got kind='%c', expected '%c'", response.Kind, expected.Kind)
	}
	if expected.Version > 0 && expected.Version != response.Version {
		ok = false
		errstr += " Version mismatch"
	}
	if response.Kind == 'C' {
		if expected.Contents != nil &&
			bytes.Compare(response.Contents, expected.Contents) != 0 {
			ok = false
		}
	}
	if !ok {
		t.Fatal("Expected " + errstr)
	}
}

func TestRPC_BasicSequential(t *testing.T) {
	initSystem()
	leaderURL := findLeader(t)
	cl := mkClient(t, leaderURL)
	defer cl.close()

	// Read non-existent file cs733net
	m, err := cl.read("cs733net")
	expectMessage(t, m, &Msg{Kind: 'F'}, "file not found", err)

	// Read non-existent file cs733net
	m, err = cl.delete("cs733net")
	expectMessage(t, m, &Msg{Kind: 'F'}, "file not found", err)
	// Write file cs733net
	data := "Cloud fun"
	m, err = cl.write("cs733net", data, 0)
	expectMessage(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = cl.read("cs733net")
	expectMessage(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

	// CAS in new value
	version1 := m.Version
	data2 := "Cloud fun 2"
	// Cas new value
	m, err = cl.cas("cs733net", version1, data2, 0)
	expectMessage(t, m, &Msg{Kind: 'O'}, "cas success", err)

	// Expect to read it back
	m, err = cl.read("cs733net")
	expectMessage(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "read my cas", err)

	// Expect Cas to fail with old version
	m, err = cl.cas("cs733net", version1, data, 0)
	expectMessage(t, m, &Msg{Kind: 'V'}, "cas version mismatch", err)

	// Expect a failed cas to not have succeeded. Read should return data2.
	m, err = cl.read("cs733net")
	expectMessage(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "failed cas to not have succeeded", err)

	// delete
	m, err = cl.delete("cs733net")
	expectMessage(t, m, &Msg{Kind: 'O'}, "delete success", err)

	// Expect to not find the file
	m, err = cl.read("cs733net")
	expectMessage(t, m, &Msg{Kind: 'F'}, "file not found", err)

}

func TestRPC_Binary(t *testing.T) {
	leaderURL := findLeader(t)
	cl := mkClient(t, leaderURL)
	defer cl.close()

	// Write binary contents
	data := "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
	m, err := cl.write("binfile", data, 0)
	expectMessage(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = cl.read("binfile")
	expectMessage(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

}

func TestRPC_Chunks(t *testing.T) {
	leaderURL := findLeader(t)
	// Should be able to accept a few bytes at a time
	cl := mkClient(t, leaderURL)
	defer cl.close()
	var err error
	snd := func(chunk string) {
		if err == nil {
			err = cl.send(chunk)
		}
	}

	// Send the command "write teststream 10\r\nabcdefghij\r\n" in multiple chunks
	// Nagle's algorithm is disabled on a write, so the server should get these in separate TCP packets.
	snd("wr")
	time.Sleep(10 * time.Millisecond)
	snd("ite test")
	time.Sleep(10 * time.Millisecond)
	snd("stream 1")
	time.Sleep(10 * time.Millisecond)
	snd("0\r\nabcdefghij\r")
	time.Sleep(10 * time.Millisecond)
	snd("\n")
	var m *Msg
	m, err = cl.rcv()
	expectMessage(t, m, &Msg{Kind: 'O'}, "writing in chunks should work", err)

}

func TestRPC_Batch(t *testing.T) {
	// Send multiple commands in one batch, expect multiple responses
	leaderURL := findLeader(t)
	cl := mkClient(t, leaderURL)
	defer cl.close()
	cmds := "write batch1 3\r\nabc\r\n" +
		"write batch2 4\r\ndefg\r\n" +
		"read batch1\r\n"

	cl.send(cmds)
	m, err := cl.rcv()
	expectMessage(t, m, &Msg{Kind: 'O'}, "write batch1 success", err)
	m, err = cl.rcv()
	expectMessage(t, m, &Msg{Kind: 'O'}, "write batch2 success", err)
	m, err = cl.rcv()
	expectMessage(t, m, &Msg{Kind: 'C', Contents: []byte("abc")}, "read batch1", err)

}

func TestRPC_BasicTimer(t *testing.T) {
	leaderURL := findLeader(t)
	cl := mkClient(t, leaderURL)
	defer cl.close()

	// Write file cs733, with expiry time of 2 seconds
	str := "Cloud fun"
	m, err := cl.write("cs733", str, 2)
	expectMessage(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back immediately.
	m, err = cl.read("cs733")
	expectMessage(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "read my cas", err)

	time.Sleep(3 * time.Second)

	// Expect to not find the file after expiry
	m, err = cl.read("cs733")
	expectMessage(t, m, &Msg{Kind: 'F'}, "file not found", err)

	// Recreate the file with expiry time of 1 second
	m, err = cl.write("cs733", str, 1)
	expectMessage(t, m, &Msg{Kind: 'O'}, "file recreated", err)

	// Overwrite the file with expiry time of 4. This should be the new time.
	m, err = cl.write("cs733", str, 4)
	expectMessage(t, m, &Msg{Kind: 'O'}, "file overwriten with exptime=4", err)

	// The last expiry time was 3 seconds. We should expect the file to still be around 2 seconds later
	time.Sleep(1 * time.Second)

	// Expect the file to not have expired.
	m, err = cl.read("cs733")
	expectMessage(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "file to not expire until 4 sec", err)

	time.Sleep(3 * time.Second)
	// 5 seconds since the last write. Expect the file to have expired
	m, err = cl.read("cs733")
	expectMessage(t, m, &Msg{Kind: 'F'}, "file not found after 4 sec", err)

	// Create the file with an expiry time of 2 sec. We're going to delete it
	// then immediately create it. The new file better not get deleted.
	m, err = cl.write("cs733", str, 3)
	expectMessage(t, m, &Msg{Kind: 'O'}, "file created for delete", err)

	m, err = cl.delete("cs733")
	expectMessage(t, m, &Msg{Kind: 'O'}, "deleted ok", err)

	m, err = cl.write("cs733", str, 0) // No expiry
	expectMessage(t, m, &Msg{Kind: 'O'}, "file recreated", err)

	time.Sleep(3100 * time.Millisecond) // A little more than 1 sec
	m, err = cl.read("cs733")
	expectMessage(t, m, &Msg{Kind: 'C'}, "file should not be deleted", err)

}

//
// nclients write to the same file. At the end the file should be
// any some clients' last write

func TestRPC_ConcurrentWrites(t *testing.T) {
	leaderURL := findLeader(t)
	nclients := 3
	niters := 5
	clients := make([]*Client, nclients)

	for i := 0; i < nclients; i++ {
		cl := mkClient(t, leaderURL)
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}

	errCh := make(chan error, nclients)
	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to begin concurrently
	sem.Add(1)
	ch := make(chan *Msg, nclients*niters) // channel for all replies
	for i := 0; i < nclients; i++ {
		go func(i int, clt *Client) {
			sem.Wait()
			cl := clt
			for j := 0; j < niters; j++ {
				//fmt.Println("Starting : ", i, " : ", j)
				str := fmt.Sprintf("cl %d %d", i, j)
				//fmt.Println("Contents to be written : ", str)

				var m *Msg
				var err error
				cl, m, err = retryWrite(t, cl, "concWrite", str, 0)
				//defer cl.close()
				if err != nil {
					errCh <- err
				} else {
					//fmt.Println("Wrote without an error")
					ch <- m
				}


			}
			//fmt.Println("Client number ", i, " done.")
		}(i, clients[i])
	}
	time.Sleep(5000 * time.Millisecond) // give goroutines a chance
	sem.Done()                          // Go!

	// There should be no errors
	for i := 0; i < nclients*niters; i++ {
		select {
		case m := <-ch:
			//fmt.Println("Done with : ", i, string(m.Contents))
			if m.Kind != 'O' {
				t.Fatalf("Concurrent write failed with kind=%c", m.Kind)
			}
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	cl := mkClient(t, leaderURL)
	m, _ := cl.read("concWrite")
	//fmt.Println("Error while reading after all writes : ", err)
	// Ensure the contents are of the form "cl <i> 9"
	// The last write of any client ends with " 9"
	//fmt.Println("Content finally read : ", string(m.Contents))
	//fmt.Println("Final Message kind for concurrent writes : ", m)
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 4")) {
		t.Fatalf("Expected to be able to read after 15 writes. Got msg ", m.Kind, " ", string(m.Contents))
	}

}

// nclients cas to the same file. At the end the file should be any one clients' last write.
// The only difference between this test and the ConcurrentWrite test above is that each
// client loops around until each CAS succeeds. The number of concurrent clients has been
// reduced to keep the testing time within limits.
func TestRPC_ConcurrentCas(t *testing.T) {
	nclients := 3
	niters := 5
	leaderURL := findLeader(t)
	clients := make([]*Client, nclients)
	for i := 0; i < nclients; i++ {
		cl := mkClient(t, leaderURL)
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}

	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to *begin* concurrently
	sem.Add(1)

	m, _ := clients[0].write("concCas", "first", 0)
	ver := m.Version
	if m.Kind != 'O' || ver == 0 {
		t.Fatalf("Expected write to succeed and return version")
	}

	var wg sync.WaitGroup
	wg.Add(nclients)

	errorCh := make(chan error, nclients)

	for i := 0; i < nclients; i++ {
		go func(i int, ver int, cl *Client) {
			sem.Wait()
			defer wg.Done()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				for {
					var m *Msg
					var err error
					cl, m, err = retryCAS(t, cl, "concCas", ver, str, 0)
					//defer cl.close()
					if err != nil {
						errorCh <- err
						return
					} else if m.Kind == 'O' {
						break
					} else if m.Kind != 'V' {
						errorCh <- errors.New(fmt.Sprintf("Expected 'V' msg, got %c", m.Kind))
						return
					}
					ver = m.Version // retry with latest version
				}
			}
		}(i, ver, clients[i])
	}

	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Start goroutines
	wg.Wait()                          // Wait for them to finish
	select {
	case e := <-errorCh:
		t.Fatalf("Error received while doing cas: %v", e)
	default: // no errors
	}
	cl := mkClient(t, leaderURL)
	m, _ = cl.read("concCas")
	//fmt.Println("Error in reading finnally : ", err)
	//fmt.Println("Final Message kind for concurrent CAS : ", m)
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 4")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg.Kind = %d, msg.Contents=%s", m.Kind, m.Contents)
	}

}

func TestRPC_KillLeader(t *testing.T) {
	leaderURL := findLeader(t)
	cl := mkClient(t, leaderURL)
	defer cl.close()
	str := "Cloud fun"
	m, err := cl.write("killLeader", str, 0)
	expectMessage(t, m, &Msg{Kind: 'O'}, "write success", err)

	//Wait for commits to propogate
	time.Sleep(2*time.Second)

	//fmt.Println("Shutting down the leader now")
	srvrs[100].shutdown()
	//fmt.Println("Done shutting down")
	time.Sleep(120*time.Second)
	// Wait fot things to settle up
	//fmt.Println("Reached here")
	leaderURL = findLeader(t)
	//fmt.Println("New leader : ", leaderURL)
	cl2 := mkClient(t, leaderURL)
	m, err = cl2.read("killLeader")
	expectMessage(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "read my write", err)
	//shutdownAll()
	srvrs[200].shutdown()
	srvrs[300].shutdown()
	srvrs[400].shutdown()
	srvrs[500].shutdown()
	//fmt.Println("All done!")
}


// Client side Retryable CAS request. Use the client variable returned for further requests
func retryCAS(t *testing.T, cl *Client, file string, version int, contents string, exptime int) (*Client, *Msg, error) {
	for {
		m, err := cl.cas(file, version, contents, exptime)
		if m.Kind == 'L' {
			leaderURL := string(m.Contents)
			if leaderURL != "_" {
				cl.close()
				cl = mkClient(t, leaderURL)
			}
			time.Sleep(50 * time.Millisecond)
		} else {
			return cl, m, err
		}

	}
}

// Client side Retryable Write request. Use the client variable returned for further requests
func retryWrite(t *testing.T, cl *Client, file string, contents string, exptime int) (*Client, *Msg, error) {
	for {
		m, err := cl.write(file, contents, exptime)
		if m.Kind == 'L' {
			leaderURL := string(m.Contents)
			if leaderURL != "_" {
				cl.close()
				cl = mkClient(t, leaderURL)
			}
			time.Sleep(50 * time.Millisecond)
		} else {
			return cl, m, err
		}

	}

}

//----------------------------------------------------------------------
// Utility functions

type Msg struct {
	// Kind = the first character of the command. For errors, it
	// is the first letter after "ERR_", ('V' for ERR_VERSION, for
	// example), except for "ERR_CMD_ERR", for which the kind is 'M'
	Kind     byte
	Filename string
	Contents []byte
	Numbytes int
	Exptime  int // expiry time in seconds
	Version  int
}

func (cl *Client) read(filename string) (*Msg, error) {
	cmd := "read " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) write(filename string, contents string, exptime int) (*Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
	} else {
		cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) cas(filename string, version int, contents string, exptime int) (*Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
	} else {
		cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) delete(filename string) (*Msg, error) {
	cmd := "delete " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

var errNoConn = errors.New("Connection is closed")

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
}

func mkClient(t *testing.T, addr string) *Client {
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err == nil {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn)}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func (cl *Client) send(str string) error {
	if cl.conn == nil {
		return errNoConn
	}
	_, err := cl.conn.Write([]byte(str))
	if err != nil {
		err = fmt.Errorf("Write error in SendRaw: %v", err)
		cl.conn.Close()
		cl.conn = nil
	}
	return err
}

func (cl *Client) sendRcv(str string) (msg *Msg, err error) {
	if cl.conn == nil {
		return nil, errNoConn
	}
	err = cl.send(str)
	if err == nil {
		msg, err = cl.rcv()
	}
	return msg, err
}

func (cl *Client) close() {
	if cl != nil && cl.conn != nil {
		cl.conn.Close()
		cl.conn = nil
	}
}

func (cl *Client) rcv() (msg *Msg, err error) {
	// we will assume no errors in server side formatting
	line, err := cl.reader.ReadString('\n')
	if err == nil {
		msg, err = parseFirst(line)
		if err != nil {
			return nil, err
		}
		if msg.Kind == 'C' {
			contents := make([]byte, msg.Numbytes)
			var c byte
			for i := 0; i < msg.Numbytes; i++ {
				if c, err = cl.reader.ReadByte(); err != nil {
					break
				}
				contents[i] = c
			}
			if err == nil {
				msg.Contents = contents
				cl.reader.ReadByte() // \r
				cl.reader.ReadByte() // \n
			}
		}
	}
	if err != nil {
		cl.close()
	}
	return msg, err
}

func parseFirst(line string) (msg *Msg, err error) {
	fields := strings.Fields(line)
	msg = &Msg{}

	// Utility function fieldNum to int
	toInt := func(fieldNum int) int {
		var i int
		if err == nil {
			if fieldNum >= len(fields) {
				err = errors.New(fmt.Sprintf("Not enough fields. Expected field #%d in %s\n", fieldNum, line))
				return 0
			}
			i, err = strconv.Atoi(fields[fieldNum])
		}
		return i
	}

	if len(fields) == 0 {
		return nil, errors.New("Empty line. The previous command is likely at fault")
	}
	switch fields[0] {
	case "OK": // OK [version]
		msg.Kind = 'O'
		if len(fields) > 1 {
			msg.Version = toInt(1)
		}
	case "CONTENTS": // CONTENTS <version> <numbytes> <exptime> \r\n
		msg.Kind = 'C'
		msg.Version = toInt(1)
		msg.Numbytes = toInt(2)
		msg.Exptime = toInt(3)
	case "ERR_VERSION":
		msg.Kind = 'V'
		msg.Version = toInt(1)
	case "ERR_FILE_NOT_FOUND":
		msg.Kind = 'F'
	case "ERR_CMD_ERR":
		msg.Kind = 'M'
	case "ERR_INTERNAL":
		msg.Kind = 'I'
	case "ERR_REDIRECT":
		msg.Kind = 'L'
		msg.Contents = []byte(fields[1])
	default:
		err = errors.New("Unknown response " + fields[0])
	}
	if err != nil {
		return nil, err
	} else {
		return msg, nil
	}
}
