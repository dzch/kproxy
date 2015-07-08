package kproxy

import (
		"time"
	   )

type Config struct {
	// kproxy
	confFile string
	logDir string
	logLevel int
	producerNum int
	// http server
	httpServerListenPort uint16
	httpServerReadTimeout time.Duration
	httpServerWriteTimeout time.Duration
	cmDataPoolSize int
	// producer
	brokerList []string
	producerMsgPoolSize int
}

func newConfig(confFile string) (*Config, error) {
    c := &Config {
		// kproxy
		confFile: confFile,
		logDir: "./log",
		logLevel: 16,
		producerNum: 1,
		// http server
		httpServerListenPort: 8088,
		httpServerReadTimeout: 10 * time.Second,
		httpServerWriteTimeout: 10 * time.Second,
		cmDataPoolSize: 10240,
		// producer
		brokerList: []string{"10.1.1.17:9902"},
		producerMsgPoolSize: 10240,
	   }
	err := c.init()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) init() error {
	return nil
}
