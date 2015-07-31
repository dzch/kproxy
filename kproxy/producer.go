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
		"github.com/Shopify/sarama"
	   )

type Producer struct {
	config *Config
	fatalErrorChan chan *error
	cmDataChan chan *CmData
	ap sarama.AsyncProducer
	pmp *PMsgPool     // Be carefull, NO lock !
}

func newProducer(kp *KProxy) (*Producer, error) {
    p := &Producer {
            config: kp.config,
		    fatalErrorChan: kp.fatalErrorChan,
			cmDataChan: kp.cmDataChan,
			pmp: newPMsgPool(kp.config.producerMsgPoolSize),
	   }
	err := p.init()
	if err != nil {
		return nil, err
	}
    return p, nil
}

func (p *Producer) init() error {
	var err error
	sarama.Logger = newSaramaLogger()
    pconfig := sarama.NewConfig()
	pconfig.Net.MaxOpenRequests = 10240
	pconfig.Producer.Return.Successes = true
	pconfig.Producer.Return.Errors = true
	pconfig.Producer.Partitioner = sarama.NewHashPartitioner
	p.ap, err = sarama.NewAsyncProducer(p.config.brokerList, pconfig)
	if err != nil {
		return err
	}
	return nil
}

func (p *Producer) run() {
	defer p.ap.Close()
	for {
		select {
			case cmData := <-p.cmDataChan:
				p.produce(cmData)
			case perr := <-p.ap.Errors():
				p.processProduceErrors(perr)
			case psucc := <-p.ap.Successes():
				p.processProduceSuccesses(psucc)
		}
	}
}

func (p *Producer) produce(cmData *CmData) {
	// fetch and fill
    pmpe := p.pmp.fetch()
	pmpe.privData = cmData
	pmsg := pmpe.pmsg
	pmsg.Topic = cmData.topic
	if len(cmData.key) == 0 {
		// if key is empty, using sarama.RandomPartitioner
		pmsg.Key = nil
	} else {
	    pmsg.Key = sarama.StringEncoder(cmData.key)
	}
	pmsg.Value = sarama.ByteEncoder(cmData.data)
	pmsg.Metadata = pmpe
	// do produce
	for {
		select {
			case p.ap.Input() <-pmsg:
				return
			case perr := <-p.ap.Errors():
				p.processProduceErrors(perr)
		}
	}
}

func (p *Producer) processProduceErrors(perr *sarama.ProducerError) {
	// fetch
    pmsg := perr.Msg
	pmpe := pmsg.Metadata.(*PMsgPoolEle)
	cmData := pmpe.privData.(*CmData)
	// put
	p.pmp.put(pmpe)
	// notice
	cmData.err = &perr.Err
	cmData.cmDoneChan <- 1
}

func (p *Producer) processProduceSuccesses(psucc *sarama.ProducerMessage) {
	// fetch
    pmpe := psucc.Metadata.(*PMsgPoolEle)
	cmData := pmpe.privData.(*CmData)
	// put
	p.pmp.put(pmpe)
	// notice
	cmData.err = nil
	cmData.offset = psucc.Offset
	cmData.partition = psucc.Partition
	cmData.cmDoneChan <- 1
}
