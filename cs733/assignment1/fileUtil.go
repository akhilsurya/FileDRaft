package main

import (
    "os"
    "bufio"
)


func GetFile (fileName string) (File, bool) {
	file, exists := fileList[fileName]
	return file, exists
}


// Taken from http://stackoverflow.com/questions/1821811/how-to-read-write-from-to-file
func WriteFile (fileName string, content []byte) string {
	// open output file
    fo, err := os.Create(fileName)
    if err != nil {
        return "INTERNAL ERROR\r\n"
    }

    defer func() {
        if err := fo.Close(); err != nil {
            panic(err)
        }
    }()
    
    w := bufio.NewWriter(fo)
    
    if _, err := w.Write(content[:len(content)]); err != nil {
        return "INTERNAL ERROR\r\n"
    }

    if err := w.Flush(); err != nil {
        return "INTERNAL ERROR\r\n"
    }

    return ""
}


func DeleteFile (fileName string) string {
    err:= os.Remove(fileName)
    if err != nil {
        return "INTERAL_ERROR\r\n"
    }
    return ""
}