package main
import (
	"os"
	"time"
	"os/exec"
	"fmt"
	"encoding/json"
)
func getLogFile(logName string) *os.File {
	var pFile *os.File = nil
	logPostFix := time.Now().Format("2006-01-02")
	logFullName := logName + "." + logPostFix
	pFile = mapLogNameToLogFile[logFullName]
	if pFile == nil {
		var err error
		pFile, err = os.OpenFile(logFullName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		command := "ln -s -f " + logFullName + " " + logName
		cmd := exec.Command("/bin/sh", "-c", command)
		if _, err := cmd.StdoutPipe(); err != nil {
			fmt.Printf("StdoutPipe: " + err.Error())
			return nil
		}
		if err := cmd.Start(); err != nil {
			fmt.Errorf("Start: ", err.Error())
			return nil
		}
		if err := cmd.Wait(); err != nil {
			fmt.Errorf("Wait: ", err.Error())
			return nil
		}
		if err != nil {
			fmt.Printf("Cant OpenFile: %s Err: %s", logName, err)
			return nil
		} else {
			mapLogNameToLogFile[logFullName] = pFile
			return pFile
		}
	}

	return pFile
}

func worker() {
	for {
		bts := <- channel
		var res map[string]interface{}

		if err := json.Unmarshal(bts, &res); err != nil {
			fmt.Printf("json unmarshal failed:", err)
		}

		log := "\n"
		if res["path"] == nil {
			continue
		}

		if res["message"] != nil {
			log = res["message"].(string) + "\n"
		}
		logName := res["path"].(string)
		pFile := getLogFile(logName)
		if pFile == nil {
			fmt.Printf("Cant Get Log File: %s\n", logName)
			continue
		}
		if _, err := pFile.WriteString(log); err != nil {
			fmt.Printf("WriteFile(%s) Err: %s Line: %s\n", logName, err, log)
			pFile.Close()
			delete(mapLogNameToLogFile, logName)
			continue
		}
	}
}



