package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/akhilsurya/fs"
	"net"
	"os"
	"strconv"
	"math/rand"
	"time"
)

var crlf = []byte{'\r', '\n'}

type ServerConfig struct {
	nodeConfig Config
	srvrAddrs  []NetConfig
	// Address and port for client connection
	addr string
	port int
	id   int
}

type FileServer struct {
	rn         Node
	nodeURLs   map[int]string
	clientConn map[string]*net.TCPConn
	addr       string
	port       int
	id         int
}

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func reply(conn *net.TCPConn, msg *fs.Msg) bool {

	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	case 'L':
		resp = string(msg.Contents)
	default:
		//fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil
}

func (srvr *FileServer) serve(conn *net.TCPConn, clientID int) {
	reader := bufio.NewReader(conn)
	for {
		msg, msgerr, fatalerr := fs.GetMsg(reader)
		leader := srvr.rn.LeaderId()
		if leader != srvr.id {
			//fmt.Println(srvr.id, " : To client ID ", msg, " : Not the Leader, sending an L ")
			if leader == -1 {
				// Don't know the leader
				//fmt.Println("Do not know the current leader")
				reply(conn, &fs.Msg{Kind: 'L', Contents: []byte("ERR_REDIRECT _")})
			} else {
				content := []byte("ERR_REDIRECT " + srvr.nodeURLs[leader])
				//fmt.Println("Redirecting to correct URL : ",  srvr.nodeURLs[leader])
				reply(conn, &fs.Msg{Kind: 'L', Contents: content, Numbytes: len(content)})
			}
			break
		}

		if fatalerr != nil || msgerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})
			conn.Close()
			break
		}
		msg.ClientID = clientID
		msg.ServerID = srvr.id
		data, err := json.Marshal(msg)
		//fmt.Println("Marshalled data : ", string(data))
		check(err)
		srvr.rn.Append(data)

	}
}

func (srvr *FileServer) commitHandler() {
	for {
		commitInfo := <-srvr.rn.CommitChannel()
		if commitInfo.Err == nil {
			var msg fs.Msg
			err := json.Unmarshal(commitInfo.Data, &msg)
			check(err)
			//log.Println(srvr.id, " : Commit info from data with client data : ", msg.ClientID)
			response := fs.ProcessMsg(&msg)
			//log.Println(srvr.id, " : Commit info for request from: ", concat(response.ServerID, response.ClientID))
			if conn, ok := srvr.clientConn[concat(response.ServerID, response.ClientID)]; ok {
				//log.Println(srvr.id, " : Replying to : ", response.ClientID)
				if !reply(conn, response) {
					//fmt.Println("Closing connection : ", concat(response.ServerID, response.ClientID))
					conn.Close()
					break
				}
			} else {
				//fmt.Println("Trying to send from wrong server")
			}

		} else {
			//fmt.Println("NO ENTRY ZONE : ", commitInfo.Err)
		}
	}
}

func (srvr *FileServer) shutdown() {
	srvr.rn.Shutdown()
}

// more of an init server
func serverMain(config ServerConfig) *FileServer {
	tcpaddr, err := net.ResolveTCPAddr("tcp", config.addr+":"+strconv.Itoa(config.port))
	check(err)
	rn, err := New(config.nodeConfig)
	check(err)
	srvr := FileServer{rn, make(map[int]string), make(map[string]*net.TCPConn), config.addr, config.port, config.id}
	// Saving URLs of others for redirect
	for _, srvrAddr := range config.srvrAddrs {
		srvr.nodeURLs[srvrAddr.Id] = srvrAddr.Host + ":" + strconv.Itoa(srvrAddr.Port)
	}

	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)
	clientID := 1
	// getting ready to handle commits
	go srvr.commitHandler()
	go func() {
		for {
			tcp_conn, err := tcp_acceptor.AcceptTCP()
			check(err)
			srvr.clientConn[concat(srvr.id, clientID)] = tcp_conn
			//fmt.Println("serving conn corresponding to ", clientID)
			go srvr.serve(tcp_conn, clientID)
			clientID++
		}
	}()
	return &srvr
}
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

func main() {
	rand.Seed(123)
	start()
	defer done()
	for {

	}
}


func concat(a int, b int ) string {
	return strconv.Itoa(a)+"_"+strconv.Itoa(b)
}