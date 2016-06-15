package main
import (
	"os"
	"time"
	"os/exec"
	"fmt"
	"encoding/json"
	"net/http"
	"backend/common"
)

//获取单个文件的大小
func backupLog(bts []byte) error {
	var isNeedBackUp bool = false
	var res map[string]interface{}

	if err := json.Unmarshal(bts, &res); err != nil {
		return &LogSinkError{Code: http.StatusInternalServerError, Message : "json unmarshal failed: " + err.Error()}
	}
	if res["path"] == nil {
		return &LogSinkError{Code: http.StatusInternalServerError, Message: "the log path is empty"}
	}
	logName := res["path"].(string)

	logPostFix := time.Now().Format("2006-01-02")
	logFullName := logName + "." + logPostFix

	common.Logger.Debug("Starting judge the need for the backup of file: %s", logFullName)
	fileInfo, err := os.Stat(logFullName)
	if err != nil {
		return &LogSinkError{Code: http.StatusInternalServerError, Message: "stat file " + logFullName + " failed: " + err.Error()}
	}
	fileSize := fileInfo.Size() //获取size

	switch gLogUnit {
	case "KB" :
		fileSize = fileSize / 1024
		if fileSize >= gLogSize {isNeedBackUp = true}
	case "MB":
		fileSize = fileSize / (1024 * 1024)
		if fileSize >= gLogSize {isNeedBackUp = true}
	case "GB":
		fileSize = fileSize / (1024 * 1024 * 1024)
		if fileSize >= gLogSize {isNeedBackUp = true}
	}
	if isNeedBackUp {
		//如果需要备份
		t := time.Now()
		newVersion := t.Format("150405")
		newLogName := logFullName + "." + newVersion
		common.Logger.Debug("Starting backup file %s to new file %s", logFullName, newLogName)
		if err := os.Rename(logFullName, newLogName); err != nil {
			return &LogSinkError{Code: http.StatusInternalServerError, Message : "rename file error: " + err.Error()}
		}
		delete(mapLogNameToLogFile, logFullName)
	}
	return nil
}

func getLogFile(logName string) (*os.File, error) {
	var pFile *os.File = nil
	logPostFix := time.Now().Format("2006-01-02")
	logFullName := logName + "." + logPostFix
	pFile = mapLogNameToLogFile[logFullName]
	if pFile == nil {
		common.Logger.Debug("Starting open a new log file: %s", logFullName)
		var err error
		pFile, err = os.OpenFile(logFullName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, &LogSinkError{Code: http.StatusInternalServerError, Message : "Cannot OpenFile " + logFullName + " , Err: " + err.Error()}
		}
		mapLogNameToLogFile[logFullName] = pFile

		//为新打开的文件建立软链接
		command := "ln -s -f " + logFullName + " " + logName
		cmd := exec.Command("/bin/sh", "-c", command)
		if _, err := cmd.StdoutPipe(); err != nil {
			return nil, &LogSinkError{Code: http.StatusInternalServerError, Message : "StdoutPipe: " + err.Error()}
		}
		if err := cmd.Start(); err != nil {
			return nil, &LogSinkError{Code: http.StatusInternalServerError, Message : "Start: " + err.Error()}
		}
		if err := cmd.Wait(); err != nil {
			return nil, &LogSinkError{Code: http.StatusInternalServerError, Message : "Wait: " + err.Error()}
		}
	}

	return pFile, nil
}

func writeLog(bts []byte) error{
	var res map[string]interface{}

	if err := json.Unmarshal(bts, &res); err != nil {
		return &LogSinkError{Code: http.StatusInternalServerError, Message : "json unmarshal failed: " + err.Error()}
	}

	log := "\n"
	if res["path"] == nil {
		return &LogSinkError{Code: http.StatusInternalServerError, Message: "the log path is empty"}
	}

	if res["message"] != nil {
		log = res["message"].(string) + "\n"
	}
	logName := res["path"].(string)
	pFile, err := getLogFile(logName)

	if pFile == nil || err != nil{
		return &LogSinkError{Code: http.StatusInternalServerError, Message: "Cannot Get Log File: " + logName + ", Err: " + err.Error()}
	}
	if _, err := pFile.WriteString(log); err != nil {
		fmt.Printf("", logName, err, log)
		pFile.Close()
		delete(mapLogNameToLogFile, logName)
		return &LogSinkError{Code: http.StatusInternalServerError, Message: "WriteFile " + logName + " Err: " + err.Error() + " Line: " + log}
	}
	return nil
}


func writer(channel chan []byte) {
	//timer := time.NewTicker(60 * time.Second)
	for {
		/**
		select {
		case <-timer.C:
			//判断pFile文件大小,如果超过限制则备份文件,并且打开一个新文件用于写
			bts := <- channel
			if err := backupLog(bts); err != nil {
				common.Logger.Error(err.Error())
			}
			if err := writeLog(bts); err != nil {
				common.Logger.Error(err.Error())
			}
		default:
			//从channel读取数据,写入文件里
			bts := <- channel
			if err := writeLog(bts); err != nil {
				common.Logger.Error(err.Error())
			}
		}**/
		bts := <- channel
		if err := writeLog(bts); err != nil {
			common.Logger.Error(err.Error())
		}
	}
}