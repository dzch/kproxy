package kproxy

import (
		"github.com/dzch/go-utils/logger"

		"os"
		"time"
	   )

type KProxy struct {
	confFile string
	config *Config
	httpServer *HttpServer
	producer *Producer
	fatalErrorChan chan *error
	cmDataChan chan *CmData
	producers []*Producer
}

var (
		cmDataChanSize = 64
	)

func NewKProxy(confFile string) (*KProxy, error) {
    kp := &KProxy {
            confFile: confFile,
		}
	err := kp.init()
	if err != nil {
		return nil, err
	}
	return kp, nil
}

func (kp *KProxy) init() error {
	var err error
	err = kp.initConfig()
	if err != nil {
		return err
	}
	err = kp.initLog()
	if err != nil {
		return err
	}
	err = kp.initChans()
	if err != nil {
		return err
	}
    err = kp.initHttpServer()
	if err != nil {
	    return err
	}
	err = kp.initProducer()
	if err != nil {
		return err
	}
	return nil
}

func (kp *KProxy) initLog() error {
    return logger.Init(kp.config.logDir, "kproxy", logger.LogLevel(kp.config.logLevel))
}

func (kp *KProxy) initConfig() error {
	var err error
    kp.config, err = newConfig(kp.confFile)
	if err != nil {
		return err
	}
	return nil
}

func (kp *KProxy) initChans() error {
	kp.fatalErrorChan = make(chan *error, 1)
	kp.cmDataChan = make(chan *CmData, cmDataChanSize)
	return nil
}

func (kp *KProxy) initHttpServer() error {
	var err error
	kp.httpServer, err = newHttpServer(kp)
	if err != nil {
		return err
	}
	return nil
}

func (kp *KProxy) initProducer() error {
	for i := 0; i < kp.config.producerNum; i ++ {
	    producer, err := newProducer(kp)
	    if err != nil {
		    return err
	    }
		kp.producers = append(kp.producers, producer)
	}
	return nil
}

func (kp *KProxy) Run() {
	for _, producer := range kp.producers {
	    go producer.run()
	}
	go kp.httpServer.run()
	err := <-kp.fatalErrorChan
	logger.Fatal("%s", (*err).Error())
	time.Sleep(1)
	os.Exit(1)
}

