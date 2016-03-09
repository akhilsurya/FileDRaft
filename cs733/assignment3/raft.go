package main

type CommitInfo struct  {
	Data []byte
	Index int
	Err error
}

type Config struct  {
	cluster []NetConfig
	Id int
	LogDir string
	ElectionTimeout int
	HeartbeatTimeout int
}

type NetConfig struct  {
	Id int
	Host string
	Port int
}

