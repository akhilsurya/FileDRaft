package main


type File struct {
	fileName string
	timeout int
	byteCount int
	version int64
	
	//lock *sync.RWMutex
} 