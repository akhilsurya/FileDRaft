package main

type Send struct {
	peer  int
	event interface{}
}

type Commit struct {
	index int
	data  []byte
	err   string
}

type Alarm struct {
	t int64
}

type LogStore struct {
	index int
	term  int
	data  []byte
}

type StateStore struct {
	term     int
	votedFor int
}
