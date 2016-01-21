package main

import (
	"fmt"
	"net"
	"testing"
	"time"
	"strconv"
	"bufio"
	"sync"
)



func TestDummy(t *testing.T){
	go serverMain()
	time.Sleep(time.Second)
}


func TestReadWrites(t *testing.T) {
	//go serverMain()
	conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{net.IPv4(127, 0, 0, 1), 8080, ""})
	
	if err != nil  {
		fmt.Println("LOL")
		return
	} 

	conn.Write([]byte("write test 4 4\r\ntest\r\n"))
	time.Sleep(1*time.Second)
	response := make([]byte, 1024)
	n, _ :=conn.Read(response)
	
	respGot := string(response[:n])
	respExp := "OK 1\r\n"
	expect(t, respGot, respExp)
	time.Sleep(1*time.Second)

	conn.Write([]byte("read test\r\n"))
	time.Sleep(1*time.Second)
	n , _ = bufio.NewReader(conn).Read(response) 
	respGot = string(response[:n])
	respExp = "CONTENTS 1 4 4 \r\ntest\r\n"
	
	expect(t, respGot, respExp)
}

func TestCAS(t *testing.T) {
	conn, _ := net.DialTCP("tcp", nil, &net.TCPAddr{net.IPv4(127, 0, 0, 1), 8080, ""})

	defer conn.Close()
	response := make([]byte, 1024)

	
	conn.Write([]byte("cas test 1 "))
	newContent := []byte("Hello, It's me.")
	conn.Write(intToBytes(len(newContent)))
	conn.Write([]byte(" 4\r\n"))
	conn.Write(newContent)
	conn.Write([]byte("\r\n"))
	
	n , e:= bufio.NewReader(conn).Read(response) 
	if e != nil  {
		fmt.Println("Error while CASing ", e)
	}
	time.Sleep(time.Second)
	respGot := string(response[:n])
	respExp := "OK 2\r\n"
	expect(t, respGot, respExp)
}

func TestDelete(t *testing.T) {
	conn, _ := net.DialTCP("tcp", nil, &net.TCPAddr{net.IPv4(127, 0, 0, 1), 8080, ""})

	defer conn.Close()
	content := []byte("cont")
	conn.Write([]byte("write test "))
	conn.Write([]byte(strconv.Itoa(len(content))))
	conn.Write([]byte(" 4\r\n"))
	conn.Write(content)
	conn.Write([]byte("\r\n"))
	time.Sleep(1*time.Second)
	response := make([]byte, 1024)
	n, _ :=conn.Read(response)
	
	respGot := string(response[:n])
	respExp := "OK 3\r\n"
	expect(t, respGot, respExp)
	time.Sleep(1*time.Second)
	conn.Write([]byte("delete test\r\n"))
	time.Sleep(1*time.Second)
	n , _ = bufio.NewReader(conn).Read(response) 
	time.Sleep(time.Second)
	respGot = string(response[:n])
	respExp = "OK\r\n"
	expect(t, respGot, respExp)
}


func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}

var wg sync.WaitGroup

func startClient(i int) {
	conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{net.IPv4(127, 0, 0, 1), 8080, ""})
	
	if err != nil {
		fmt.Println("Server not up")
		return
	}
	defer conn.Close()
	content := []byte("cont")
	content = append(content, []byte(strconv.Itoa(i))...)
	conn.Write([]byte("write test "))
	conn.Write([]byte(strconv.Itoa(len(content))))
	conn.Write([]byte(" 4\r\n"))
	conn.Write(content)
	conn.Write([]byte("\r\n"))
	response := make([]byte, 1024)
	time.Sleep(time.Second)
	n, _ :=conn.Read(response)
	conn.Write([]byte("read test\r\n"))
	n , _ = bufio.NewReader(conn).Read(response[:n]) 
	wg.Done()
}

func TestConcurrency(t *testing.T) {
	for i:=0; i<5; i++ {
		wg.Add(1)
		go startClient(i)
	}
	wg.Wait()
}