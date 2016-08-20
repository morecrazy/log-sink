package main
import (
	"os"
	"time"
	"os/exec"
	"fmt"
	"encoding/json"
	"net/http"
	"backend/common"
	"sync"
	"codoon_ops/log-sink/cache"
)

type File struct {
	l *sync.Mutex
	file *os.File
}

//获取单个文件的大小
//flag: 是否进行了备份操作
func backupLog(bts []byte) (flag bool, err error) {
	var isNeedBackUp bool = false
	var res map[string]interface{}

	if err := json.Unmarshal(bts, &res); err != nil {
		return false, &LogSinkError{Code: http.StatusInternalServerError, Message : "json unmarshal failed: " + err.Error()}
	}
	if res["path"] == nil {
		return false, &LogSinkError{Code: http.StatusInternalServerError, Message: "the log path is empty"}
	}
	logName := res["path"].(string)
	logPostFix := time.Now().Format("2006-01-02")
	logFullName := logName + "." + logPostFix

	myFile := mapLogNameToLogFile[logFullName]
	if myFile == nil { return false, nil }
	//开始备份操作
	myFile.l.Lock()
	defer myFile.l.Unlock()
	common.Logger.Debug("Starting judge the need for the backup of file: %s", logFullName)

	fileInfo, err := os.Stat(logFullName)
	if err != nil {
		return false, &LogSinkError{Code: http.StatusInternalServerError, Message: "stat file " + logFullName + " failed: " + err.Error()}
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
		//如果需要备份,则备份文件,且打开新文件
		t := time.Now()
		newVersion := t.Format("150405")
		newLogName := logFullName + "." + newVersion
		common.Logger.Debug("Starting backup file %s to new file %s", logFullName, newLogName)
		if err := os.Rename(logFullName, newLogName); err != nil {
			return false, &LogSinkError{Code: http.StatusInternalServerError, Message : "rename file error: " + err.Error()}
		}
		//关闭文件句柄
		mapLogNameToLogFile[logFullName].file.Close()
		//删除map中的记录
		delete(mapLogNameToLogFile, logFullName)
		//往新文件中写入数据
		err := writeLogBuf(bts)
		return true, err
	}
	return false, nil
}

func getLogFile(logName string) (*os.File, error) {
	logPostFix := time.Now().Format("2006-01-02")
	logFullName := logName + "." + logPostFix
	myFile := mapLogNameToLogFile[logFullName]
	if myFile == nil {
		common.Logger.Info("Starting open a new log file: %s", logFullName)
		var err error
		pFile, err := os.OpenFile(logFullName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, &LogSinkError{Code: http.StatusInternalServerError, Message : "Cannot OpenFile " + logFullName + " , Err: " + err.Error()}
		}
		myFile = new(File)
		myFile.file = pFile
		myFile.l = new(sync.Mutex)
		mapLogNameToLogFile[logFullName] = myFile

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

	return myFile.file, nil
}

func getLogBuffer(logName string) *LogBuffer {
	logPostFix := time.Now().Format("2006-01-02")
	logFullName := logName + "." + logPostFix
	logBuffer, found := c.Get(logFullName)
	if !found {
		//删除失效的key
		c.Delete(logFullName)
		common.Logger.Info("Creating a new logbuffer: %s", logFullName)
		logBuffer := NewLogBuffer(logName)
		//设置key过期时间
		c.Set(logFullName, logBuffer, cache.DefaultExpiration)
		go logWriter(logBuffer)
		return logBuffer
	}
	return logBuffer.(*LogBuffer)
}

//将log写入logName文件中
func writeLog(logName, log string) error {
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

func writeLogBuf(bts []byte) error{
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

	logBuffer := getLogBuffer(logName)
	if _, err := logBuffer.WriteString(log); err != nil {
		return &LogSinkError{Code: http.StatusInternalServerError, Message: "write log buffer failed!"}
	}

	return nil
}


func bufWriter(channel chan []byte) {
	timer := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-timer.C:
			//判断pFile文件大小,如果超过限制则备份文件,并且打开一个新文件用于写
			bts := <- channel
			flag, err := backupLog(bts)
			if err != nil {
				common.Logger.Error(err.Error())
			}
		    //如果没有进行备份,则将这条记录写入日志文件中
		    if flag == false {
				if err := writeLogBuf(bts); err != nil {
					common.Logger.Error(err.Error())
				}
			}
		default:
			//从channel读取数据,写入文件里
			bts := <- channel
			if err := writeLogBuf(bts); err != nil {
				common.Logger.Error(err.Error())
			}
		}
	}
}

func logWriter(logBuffer *LogBuffer) {
	timer := time.NewTicker(1 * time.Second)
	WriterLoop:
	for {
		select {
		case <- logBuffer.ch:
			//从buf读取数据,写入文件里
			str := logBuffer.ReadString()
			if err := writeLog(logBuffer.name, str); err != nil {
				common.Logger.Error(err.Error())
			}
		case <-timer.C:
			//超时时间到,强制读取数据
			//从buf读取数据,写入文件里
			str := logBuffer.ReadString()
			if err := writeLog(logBuffer.name, str); err != nil {
				common.Logger.Error(err.Error())
			}
		case <-logBuffer.closing:
			//writer过期
			break WriterLoop
		}
	}
}