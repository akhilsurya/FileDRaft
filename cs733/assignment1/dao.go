package main

import ( "io/ioutil"; "strconv"; "time")

func Write( fileName string,  byteCount int, content []byte, timeout int) (int64, string) {

	lock.Lock()
	defer lock.Unlock()
	oldFile, exists := GetFile(fileName)
	var newVersion int64
	var newChannel chan int
	if exists {
		if oldFile.timerRunning {
			oldFile.quitTimeout<-1
		}	
		
		newVersion = oldFile.version+1
	} else {
		
		newVersion = 1
	}	
	newChannel = make(chan int)
	
	err := WriteFile(fileName, content)
	if err != "" {
		return 0, "ERR_INTERNAL\r\n"
	}
	timerRunning := false
	newDeadline := time.Now()
	if (byteCount != 0) && (timeout != -1) {
		timerRunning = true
		var nanoDur int64 
		nanoDur = int64(timeout)*1000000000
		newDeadline = time.Now().Add(time.Duration(nanoDur)*time.Nanosecond)
		go checkTimeout(newChannel, fileName, timeout)
	} 
	newFile := File{fileName, timeout, byteCount, newVersion, newChannel, newDeadline, timerRunning}
	fileList[fileName] = newFile
	
	return newVersion, ""
}

func Read(fileName string) (int64, int, int, []byte, string) {
	lock.RLock()
	defer lock.RUnlock()
	
	file, exists := GetFile(fileName)
	
	
	if exists {
		content, e := ioutil.ReadFile(fileName)
		if e != nil {
			return 0, 0, 0, content, "ERR_INTERNAL\r\n "
		} else {
			var timeout int
			timeout = -1
			if  file.timerRunning {
				dur := file.deadline.Sub(time.Now())
				timeout = int(dur.Seconds())
			}
			return file.version, len(content), timeout, content, ""
		}
		
	} else {
		
		return 0, 0, 0, make([]byte, 0), "ERR_FILE_NOT_FOUND\r\n"
	}
}


func CAS(fileName string, version int64,  byteCount int, content []byte, timeout int) (int64, string) {
	lock.Lock()
	defer lock.Unlock()


	file, exists := GetFile(fileName)

	if exists {
		if version == file.version {
			if file.timerRunning {
				file.quitTimeout<-1
			}	
			err := WriteFile(fileName, content)
			if err != "" {
				return 0, "ERR_INTERNAL\r\n"
			}
			newChannel := make(chan int)
			newVersion := version+1
			timerRunning := false
			newDeadline := time.Now()

			if (byteCount != 0) && (timeout != -1) {
				timerRunning = true
				nanoDur := int64(timeout)*1000000000
				newDeadline = time.Now().Add(time.Duration(nanoDur)*time.Second)
				go checkTimeout(newChannel, fileName, timeout)
			} 
			newFile := File{fileName, timeout, byteCount, newVersion, newChannel, newDeadline, timerRunning}
			fileList[fileName] = newFile
			
			return newVersion, ""
		} else {
			return 0, "ERR_VERSION "+strconv.FormatInt(file.version, 10)
		}
	} else {
		return 0, "ERR_FILE_NOT_FOUND\r\n" 
	}



}	

func Delete(fileName string) string {
	lock.Lock()

	defer lock.Unlock()

	file, exists := GetFile(fileName)
	if !exists {
		return "ERR_FILE_NOT_FOUND\r\n"
	}

	err := DeleteFile(fileName)
	if err != "" {
		return "ERR_INTERNAL\r\n"
	}
	file.quitTimeout<-1
	delete(fileList, fileName)
	return ""
}