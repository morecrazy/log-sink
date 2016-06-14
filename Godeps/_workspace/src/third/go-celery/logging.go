package celery

import (
	"third/go-logging"
)

var logger *logging.Logger

func RegisterLogger(g_logger *logging.Logger) {
	logger = g_logger
}

func GetLogger() *logging.Logger {
	return logger
}
