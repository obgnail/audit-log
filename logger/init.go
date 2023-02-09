package logger

import (
	"github.com/juju/errors"
	cfg "github.com/obgnail/audit-log/config"
	"io"
	"os"
	"strings"
)

var CommonLogger *Logger

func InitLogger() error {
	sep := "/github.com"
	pathPrefix := "/github.com/obgnail/audit-log"

	logFile, err := os.OpenFile(cfg.Main.Log.LogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		return errors.Trace(err)
	}

	logLevel := InfoLevel
	switch strings.ToUpper(cfg.Main.Log.LogLevel) {
	case "TRACE":
		logLevel = TraceLevel
	case "INFO":
		logLevel = InfoLevel
	case "WARN":
		logLevel = WarnLevel
	case "ERROR":
		logLevel = ErrorLevel
	}

	auditConfig := Config{
		Sep:        sep,
		Level:      logLevel,
		Target:     io.MultiWriter(os.Stdout, logFile),
		PathPrefix: pathPrefix,
		Encoder:    &PlainEncoder{EnableBuffer: false}, //先禁用buffer,如果开启需要处理系统信号量
	}

	CommonLogger, _, err = NewLogger(auditConfig)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func Error(format string, args ...interface{}) {
	CommonLogger.ErrorPath(CommonLogger.GetPath(), format, args...)
}

func ErrorDetails(err error) {
	CommonLogger.ErrorDetailsPath(CommonLogger.GetPath(), err)
}

func Warn(format string, args ...interface{}) {
	CommonLogger.WarnPath(CommonLogger.GetPath(), format, args...)
}

func WarnDetails(err error) {
	CommonLogger.WarnDetailsPath(CommonLogger.GetPath(), err)
}

func Info(format string, args ...interface{}) {
	CommonLogger.InfoPath(CommonLogger.GetPath(), format, args...)
}

func Trace(format string, args ...interface{}) {
	CommonLogger.TracePath(CommonLogger.GetPath(), format, args...)
}
