/*
    The MIT License (MIT)
    
    Copyright (c) 2015 zhouwench zhouwench@gmail.com
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
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
