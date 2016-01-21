package main

import ("fmt"
	"net"
	"io"
	"bufio"
	"strconv"
	"strings"
	"sync"
	"time"
	"log"
	)

var fileList map[string]File
var lock *sync.RWMutex

func serverMain() {
	fileList = make(map[string]File)
	lock = &sync.RWMutex{}
	ln, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("Not up ")	
	}
	
	
	for {
		conn, err := ln.Accept()
		
		defer conn.Close()
		if err!= nil {
			fmt.Println("Could not accept a Connection : ", err)
		} else {
			go handleConnection(conn)
		}
		
	}
}


func badCommand(conn net.Conn) {
	_, e := conn.Write([]byte("ERR_CMD_ERR\r\n"))
	if e == nil {
		conn.Close()	
	}
}
	

func internalError(conn net.Conn) {
	conn.Write([]byte("ERR_INTERNAL\r\n"))
	
}

func int64ToBytes(n int64) []byte {
	return []byte(strconv.FormatInt(n, 10))
}

func intToBytes(n int) []byte {
	return []byte(strconv.Itoa(n))
}


func writeReadResponse(conn net.Conn, version int64, byteCount int, timeout int, contentRead []byte, err string) {
	if err != "" {
		conn.Write([]byte(err))
	} else {
		
		// Handle errors
		conn.Write([]byte("CONTENTS "))
		conn.Write(int64ToBytes(version))
		conn.Write([]byte(" "))
		conn.Write(intToBytes(byteCount))
		conn.Write([]byte(" "))
		conn.Write(intToBytes(timeout))
		conn.Write([]byte(" \r\n"))
		conn.Write(contentRead)
		conn.Write([]byte("\r\n"))

	}	
}

func writeWriteResponse(conn net.Conn, version int64, err string) {
	if err == "" {
		conn.Write([]byte("OK "))	
		conn.Write(int64ToBytes(version))
		conn.Write([]byte("\r\n"))
	} else {
		conn.Write([]byte(err))
	}
}

func writeDeleteResponse(conn net.Conn, err string) {
	if err != "" {
		conn.Write([]byte(err))
	} else {
		conn.Write([]byte("OK\r\n"))
	}
}

func writeCASResponse(conn net.Conn, err string, version int64) {
	if err != "" {
		conn.Write([]byte(err))
	} else {
		conn.Write([]byte("OK "+string(int64ToBytes(version))+"\r\n"))
	}
} 
/**
* Unclean code coming up
*/

func handleConnection(conn net.Conn) {
	temp := make([]byte, 0)
	var buf []byte
	var content []byte
	bytesLeft := 0;
	waitingCommand := "write"
	var fileName string 
	var version int64
	var timeout int
	var byteCount int 
	var err string
	for {
		var n int
		if len(temp) == 0 {
			temp = make([]byte, 256)
			var e error
			n, e = bufio.NewReader(conn).Read(temp) 
			time.Sleep(time.Millisecond)
			temp = temp[:n]
			
			if e != nil {
				if e == io.EOF {
					conn.Close()
					return;
				}
			}
		} else {
			n = len(temp)
		}


		if (bytesLeft != 0) {
			if (bytesLeft > n) {
				bytesLeft = bytesLeft -n;
				content = append(content, temp[:n]...)
				continue
			} else {
				// check /r/n ending
				content = append(content, temp[:bytesLeft]...)
				if waitingCommand == "write" {
					version, e :=Write(fileName, byteCount, content, timeout)
					writeWriteResponse(conn, version, e)
				} else if waitingCommand == "cas" {
					newVersion, e := CAS(fileName, version, byteCount, content, timeout)
					writeCASResponse(conn, e, newVersion)
				} else {
					internalError(conn)
				}
				temp = temp[bytesLeft:]
				// clear content
				content = make([]byte, 0)
				buf = make([]byte, 0)
			}
		} 

		oldBufSize := len(buf)
		buf = append(buf, temp[:n]...)
		forSearch := string(buf) 

		index := strings.Index(forSearch, "\r\n")
		if index != -1 {
			// Accounting for /r/n
			temp = temp[(index-oldBufSize+2):]

			command := string(buf[:index])

			// Process the command 
			if strings.HasPrefix(command, "read ") {
				fileName, err = ParseRead(command)
				//
				
				if err != "" {
					badCommand(conn)
					return;
				}
				temp= make([]byte, 0)
				buf = make([]byte, 0)
				version, byteCount, timeout, contentRead, e := Read(fileName)
				writeReadResponse(conn, version, byteCount, timeout, contentRead, e)

			} else if strings.HasPrefix(command, "write ") {

				fileName, byteCount, timeout, err = ParseWrite(command)
				if (err != "") {
					badCommand(conn);
				}
				waitingCommand = "write"
				
				if byteCount+2 > len(temp){
					// clean up
					content = make([]byte, 0)
					content = append(content, temp...)
					bytesLeft = byteCount+2 - len(temp)
				} else {
					content= append(content, temp[:byteCount]...)
					// ignoring /r/n
					temp = temp[(byteCount+2):]
					buf = make([]byte, 0)
					var e string
					version, e = Write(fileName, byteCount, content, timeout)
					writeWriteResponse(conn, version, e)
				}
 			} else if strings.HasPrefix(command, "cas ") {
 				fileName, version, byteCount, timeout, err = ParseCAS(command)
 				if (err != "") {
					badCommand(conn);
				}
				waitingCommand = "cas"
				
				if byteCount+2 > len(temp){
					content = append(content, temp...)
					bytesLeft = byteCount+2 - len(temp)
				} else {
					content= append(content, temp[:byteCount]...)
					temp = temp[(byteCount+2):]
					buf = make([]byte, 0)
					newVersion, e := CAS(fileName, version, byteCount, content, timeout)
					writeCASResponse(conn, e, newVersion)
				}
			} else if strings.HasPrefix(command, "delete ") {
				fileName, err = ParseDelete(command)
				if err != "" {
					badCommand(conn)
				}
				temp= make([]byte, 0)
				buf = make([]byte, 0)
				e := Delete(fileName)
				writeDeleteResponse(conn, e)
			} else {
				// more thinking needed
				badCommand(conn)
			}

		}
	}

	
}




func main() {
  serverMain()
}
