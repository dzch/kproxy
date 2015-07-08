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
		"github.com/dzch/go-utils/logger"
		"net/http"
		"fmt"
		"errors"
		"time"
		"io"
	   )

type HttpServer struct {
	config *Config
	server *http.Server
	cmDataChan chan *CmData
	fatalErrorChan chan *error
	cdp *CmDataPool
}

func newHttpServer(kp *KProxy) (*HttpServer, error) {
	hs := &HttpServer{
            config: kp.config,
			cmDataChan: kp.cmDataChan,
			fatalErrorChan: kp.fatalErrorChan,
		}
    err := hs.init()
	if err != nil {
		return nil, err
	}
	return hs, nil
}

func (httpServer *HttpServer) init() error {
	err := httpServer.initCDP()
	if err != nil {
		return err
	}
    err = httpServer.initHttpServer()
	if err != nil {
		return err
	}
	return nil
}

func (httpServer *HttpServer) initHttpServer() error {
    mux := http.NewServeMux()
	mux.HandleFunc("/", httpServer.handleCm)
	//mux.HandleFunc("/batch", httpServer.handleCmBatch)
    httpServer.server = &http.Server {
        Addr: fmt.Sprintf(":%d", httpServer.config.httpServerListenPort),
		Handler: mux,
		ReadTimeout: httpServer.config.httpServerReadTimeout,
		WriteTimeout: httpServer.config.httpServerWriteTimeout,
	}
	return nil
}

func (httpServer *HttpServer) initCDP() error {
	httpServer.cdp = newCmDataPool(httpServer.config.cmDataPoolSize)
	return nil
}

func (httpServer *HttpServer) run() {
    err := httpServer.server.ListenAndServe()
	if err != nil {
		logger.Fatal("fail to start http server: %s", err.Error())
		httpServer.fatalErrorChan <- &err
		return
	}
	err = errors.New("http server done")
	httpServer.fatalErrorChan <- &err
}

func (httpServer *HttpServer) handleCm(w http.ResponseWriter, r *http.Request) {
    startTime := time.Now()
	/* check query */
	if r.ContentLength <= 0 {
		logger.Warning("invalid query, need post data: %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
    qv := r.URL.Query()
    post := make([]byte, r.ContentLength)
	nr, err := io.ReadFull(r.Body, post)
	if int64(nr) != r.ContentLength {
		logger.Warning("fail to read body: %s, %s", r.URL.String(), err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	/* compose CmData */
	cdpe := httpServer.cdp.fetch()
	defer httpServer.cdp.put(cdpe)
	cdpe.cmData.topic = qv.Get("topic")
	cdpe.cmData.key = qv.Get("key")
	cdpe.cmData.data = post
	/* commit */
	httpServer.cmDataChan <- cdpe.cmData
	/* wait res */
	<-cdpe.cmData.cmDoneChan
	if cdpe.cmData.err != nil {
		logger.Warning("fail to commit req: %s, error: %s", r.URL.String(), (*(cdpe.cmData.err)).Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
    endTime := time.Now()
	costTimeUS := endTime.Sub(startTime)/time.Microsecond
	// TODO
	logger.Notice("success process commit: %s, cost_us=%d, datalen=%d, offset=%d, partition=%d", r.URL.String(), costTimeUS, nr, cdpe.cmData.offset, cdpe.cmData.partition)
	w.WriteHeader(http.StatusOK)
	return
}

