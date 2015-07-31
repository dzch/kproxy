package kproxy

import (
		"github.com/dzch/go-utils/logger"
		"fmt"
	   )

type SaramaLogger struct {}

func newSaramaLogger() *SaramaLogger {
	return &SaramaLogger{}
}

func (sl *SaramaLogger) Print(v ...interface{}) {
	logger.Warning("SARAMA: %s", fmt.Sprint(v...))
}

func (sl *SaramaLogger) Printf(format string, v ...interface{}) {
	logger.Warning("SARAMA: %s", fmt.Sprintf(format, v...))
}

func (sl *SaramaLogger) Println(v ...interface{}) {
	logger.Warning("SARAMA: %s", fmt.Sprintln(v...))
}
