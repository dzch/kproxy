package kproxy

import (
		"github.com/dzch/go-utils/logger"
		"container/ring"
		"sync"
	   )

type CmData struct {
	// cm
	topic string
	key string
	data []byte
	// res
	cmDoneChan chan int
	err *error
	offset int64
	partition int32
}

type CmDataPoolEle struct {
	cmData *CmData
	r *ring.Ring
}

type CmDataPool struct {
	root *CmDataPoolEle
	poolSize int
	poolFreeSize int
	mutex sync.Mutex
}

func newCmDataPool(size int) *CmDataPool {
    cdp := &CmDataPool {
        poolSize: size,
		 }
	cdp.init()
	return cdp
}

func (cdp *CmDataPool) fetch() *CmDataPoolEle {
	cdp.mutex.Lock()
	defer cdp.mutex.Unlock()
	if cdp.poolFreeSize == 0 {
		logger.Warning("CmDataPool size is too small")
        return cdp.newCmDataPoolEle()
	}
	r := cdp.root.r.Unlink(1)
	cdp.poolFreeSize --
	return r.Value.(*CmDataPoolEle)
}

func (cdp *CmDataPool) put(cdpe *CmDataPoolEle) {
	cdp.mutex.Lock()
	defer cdp.mutex.Unlock()
	if cdp.poolFreeSize == cdp.poolSize {
		return
	}
	cdp.root.r.Link(cdpe.r)
	cdp.poolFreeSize ++
}

func (cdp *CmDataPool) init() {
	cdp.root = cdp.newCmDataPoolEle()
	r := cdp.root.r
	for i := 0; i < cdp.poolSize; i ++ {
        cdpe := cdp.newCmDataPoolEle()
		r.Link(cdpe.r)
	}
	cdp.poolFreeSize = cdp.poolSize
}

func (cdp *CmDataPool) newCmDataPoolEle() *CmDataPoolEle {
    cmData := &CmData {
		cmDoneChan: make(chan int, 1),
    }
    cdpe := &CmDataPoolEle {
        cmData: cmData,
	}
	cdpe.r = ring.New(1)
	cdpe.r.Value = cdpe
	return cdpe
}

