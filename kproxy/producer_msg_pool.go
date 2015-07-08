package kproxy

import (
		"github.com/Shopify/sarama"
		"github.com/dzch/go-utils/logger"
		"container/ring"
	   )

type PMsgPoolEle struct {
	pmsg *sarama.ProducerMessage
	privData interface{}
	r *ring.Ring
}

// no lock here
// should only be called by one routine
type PMsgPool struct {
	root *PMsgPoolEle
	poolSize int
	poolFreeSize int
}

func newPMsgPool(size int) *PMsgPool {
    pmp := &PMsgPool {
            poolSize: size,
		 }
	pmp.init()
	return pmp
}

func (pmp *PMsgPool) init() {
	pmp.root = pmp.newPMsgPoolEle()
	for i := 0; i < pmp.poolSize; i ++ {
        pmpe := pmp.newPMsgPoolEle()
		pmp.root.r.Link(pmpe.r)
	}
	pmp.poolFreeSize = pmp.poolSize
}

func (pmp *PMsgPool) newPMsgPoolEle() *PMsgPoolEle {
    pmpe := &PMsgPoolEle {
        pmsg: &sarama.ProducerMessage{},
		r: ring.New(1),
		  }
	pmpe.r.Value = pmpe
	return pmpe
}

func (pmp *PMsgPool) fetch() *PMsgPoolEle {
	if pmp.poolFreeSize <= 0 {
		logger.Warning("Producer Message Pool is too small");
		return pmp.newPMsgPoolEle()
	}
    r := pmp.root.r.Unlink(1)
	pmp.poolFreeSize --
	return r.Value.(*PMsgPoolEle)
}

func (pmp *PMsgPool) put(pmpe *PMsgPoolEle) {
	if pmp.poolFreeSize >= pmp.poolSize {
		return
	}
	pmp.root.r.Link(pmpe.r)
	pmp.poolFreeSize ++
	return
}

