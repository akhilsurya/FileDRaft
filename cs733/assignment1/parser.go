package main

import (
	"strconv"
	"strings"
)

func parseReadOrDelete(command string) (fileName string, err string) {
	parameters := strings.Split(command, " ")
	
	if len(parameters) == 2 {
		fileName = parameters[1]
		err = ""
	} else {
		fileName = ""
		err = "ERR_CMD_ERR\r\n" 
	}
	return
}

func ParseWrite(command string) (string, int, int, string)  {
	parameters := strings.Split(command, " ")
	
	if len(parameters) == 3 {
		fileName := parameters[1]
		byteCount, e := strconv.Atoi(parameters[2])
		if e != nil {
			return "",0, 0, "ERR_CMD_ERR\r\n"
 		}
		timeout := 0 // ??
		err := ""
		return fileName, byteCount, timeout,err
	} else if  len(parameters) == 4 {
		fileName := parameters[1]
		byteCount, e1 := strconv.Atoi(parameters[2])
		timeout, e2:= strconv.Atoi(parameters[3])
		if e1 != nil || e2 != nil {
			return "",0, 0, "ERR_CMD_ERR\r\n" 
		}
		err := ""
		return fileName, byteCount, timeout, err
	} else {
		return "",0, 0, "ERR_CMD_ERR\r\n" 
	}
	
}

func ParseCAS(command string) (fileName string, version int64, byteCount int, timeout int, err string)  {
	parameters := strings.Split(command, " ")
	if len(parameters) == 4 {
		var e1, e2 error
		fileName = parameters[1]
		version, e1 = strconv.ParseInt(parameters[2], 10, 64)
		
		byteCount, e2 = strconv.Atoi(parameters[3])
		if e1 != nil || e2 != nil  {
			version = 0;
			byteCount = 0;
			timeout = 0;
			err = "ERR_CMD_ERR\r\n" 
			return
		}
		timeout = 0 
		err = ""

	} else if  len(parameters) == 5 {
		var e1, e2, e3 error
		fileName = parameters[1]

		version, e1= strconv.ParseInt(parameters[2], 10, 64)
		byteCount, e2 = strconv.Atoi(parameters[3])
		timeout, e3 = strconv.Atoi(parameters[4])
		if e1 != nil || e2 != nil || e3 != nil  {
			version = 0;
			byteCount = 0;
			timeout = 0;
			err = "ERR_CMD_ERR\r\n" 
			return
		}
		err = ""
	} else {
		fileName = "";
		version = 0;
		byteCount -= 0;
		timeout = 0;
		err = "ERR_CMD_ERR\r\n" 
	}
	return
}



func ParseRead(command string) (fileName string, err string) {
	fileName, err = parseReadOrDelete(command)
	return
}


func ParseDelete(command string) (fileName string, err string) {
	fileName, err = parseReadOrDelete(command)
	return
}

