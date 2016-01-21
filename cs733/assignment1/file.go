package main
import "time"

type File struct {
	fileName string
	timeout int
	byteCount int
	version int64
	quitTimeout chan int
	deadline time.Time
	timerRunning bool
	//lock *sync.RWMutex
} 