package main

import (
	"encoding/json"
)

type LogSinkError struct {
	Code int
	Message string
}

func (err *LogSinkError) Error() string {
	error_str, _ := json.Marshal(err)
	return string(error_str)
}
