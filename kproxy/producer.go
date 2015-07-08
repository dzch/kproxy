package kproxy

import (
		"github.com/Shopify/sarama"
//		"github.com/dzch/go-utils/logger"
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
	// TODO: producer config
	var err error
    pconfig := sarama.NewConfig()
	pconfig.Net.MaxOpenRequests = 10240
	pconfig.Producer.Return.Successes = true
	pconfig.Producer.Return.Errors = true
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
	pmsg.Key = sarama.StringEncoder(cmData.key)
	pmsg.Value = sarama.ByteEncoder(cmData.data)
	pmsg.Partition = 0   // TODO: 
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
