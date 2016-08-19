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
	closing chan struct{}
}

func NewLogBuffer(s string) *LogBuffer {
	return &LogBuffer{m: new(sync.Mutex), buf: new(bytes.Buffer), len: 0, ch: make(chan bool, 1), name: s, closing:make(chan struct{})}
}

func (b *LogBuffer) WriteString(s string) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	b.len ++
	if b.len == gLogBufferSize {
		b.ch <- true
	}
	return b.buf.WriteString(s)
}

func (b *LogBuffer) ReadString() string {
	b.m.Lock()
	defer b.m.Unlock()
	str := b.buf.String()
	common.Logger.Debug("start read string from logbuffer, the log buffer name is %s, and the length is %d", b.name, b.len)
	b.buf.Reset()
	b.len = 0
	return str
}
