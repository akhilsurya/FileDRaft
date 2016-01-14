package main

import ("fmt"
	"net"
	"io"
	"bytes")

func serverMain() {
	ln, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Could not start the server : ", err)
	}
	go startClient()
	for {
		conn, err := ln.Accept()
		defer conn.Close()
		if err!= nil {
			fmt.Printf("Could not accept a Connection : ", err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Printf("Connection Accepted\r\n")
	var buf bytes.Buffer
	byteCount, err := io.Copy(&buf, conn)
	if err!= nil {
		fmt.Println("Could not retreive message")
	}
	fmt.Println(byteCount)
	fmt.Println("Message : ", buf.String())
}

func main() {
  serverMain()
}
