package main

import ( "io/ioutil"; "strconv")

func Write( fileName string,  byteCount int, content []byte, timeout int) (int64, string) {

	lock.Lock()
	defer lock.Unlock()
	oldFile, exists := GetFile(fileName)
	var newVersion int64
	if exists {
		newVersion = oldFile.version+1
	} else {
		
		newVersion = 1
	}	
	newFile := File{fileName, timeout, byteCount, newVersion}
	fileList[fileName] = newFile
	err := WriteFile(fileName, content)
	if err != "" {
		return 0, "ERR_INTERNAL\r\n"
	}
	if (byteCount != 0) || (timeout != -1) {
		// Timeout
	}
	
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
			return file.version, len(content), file.timeout, content, ""
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
			newVersion := version+1
			newFile := File{fileName, timeout, byteCount, newVersion}
			fileList[fileName] = newFile
			err := WriteFile(fileName, content)
			if err != "" {
				return 0, "ERR_INTERNAL\r\n"
			}
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

	_, exists := GetFile(fileName)
	if !exists {
		return "ERR_FILE_NOT_FOUND\r\n"
	}

	err := DeleteFile(fileName)
	if err != "" {
		return "ERR_INTERNAL\r\n"
	}
	delete(fileList, fileName)
	return ""
}