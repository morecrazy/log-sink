package main

import (
	"bytes"
	"sync"
	"backend/common"
)
type LogBuffer struct {
	m *sync.Mutex
	buf *bytes.Buffer
	len int64
	ch chan bool
	name string
}

func (b *LogBuffer) WriteString(s string) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	b.len ++
	if b.len >= gLogBufferSize {
		b.ch <- true
	}
	return b.buf.WriteString(s)
}

func (b *LogBuffer) ReadString() string {
	select {
	case <-b.ch:
		common.Logger.Debug("start read string from buffer")
		b.m.Lock()
		defer b.m.Unlock()
		str := b.buf.String()
		b.buf.Reset()
		b.len = 0
		return str
	default:
		return ""
	}
}

func (b *LogBuffer) ForceSet() {
	b.m.Lock()
	defer b.m.Unlock()
	select {
	case b.ch<- true:
		common.Logger.Debug("force restart channel")
	default:
		common.Logger.Debug("the channel is already setted")
	}
}
